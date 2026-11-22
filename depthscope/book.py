"""Binding for the C order-book processor."""

import ctypes as ct
from decimal import Decimal
from pathlib import Path

SCALE = 100_000_000


class Level(ct.Structure):
    _fields_ = [("price", ct.c_int64), ("quantity", ct.c_int64), ("digits", ct.c_char * 42)]


def load_library():
    path = Path(__file__).resolve().parents[1] / "build/libdepthscope.so"
    try:
        library = ct.CDLL(str(path))
    except OSError as exc:
        raise RuntimeError("Build the C processor with make first") from exc
    signatures = {
        "book_create": (ct.c_void_p, ct.c_size_t),
        "book_destroy": (None, ct.c_void_p),
        "book_reset": (None, ct.c_void_p, ct.c_bool),
        "book_set": (ct.c_bool, ct.c_void_p, ct.c_bool, ct.c_int64, ct.c_int64, ct.c_char_p),
        "book_finish": (ct.c_uint32, ct.c_void_p),
        "book_valid": (ct.c_bool, ct.c_void_p),
        "book_levels": (ct.c_size_t, ct.c_void_p, ct.c_bool, ct.POINTER(ct.POINTER(Level))),
    }
    for name, (result, *arguments) in signatures.items():
        function = getattr(library, name)
        function.restype = result
        function.argtypes = arguments
    return library


def fixed(value):
    value = Decimal(value)
    if not value.is_finite() or value < 0 or value >= Decimal(2**63) / SCALE:
        raise ValueError("price or quantity outside fixed-point range")
    units = value * SCALE
    if units != units.to_integral_value():
        raise ValueError("price or quantity exceeds eight decimal places")
    return int(units)


def checksum_digits(value):
    return format(Decimal(value), "f").replace(".", "").lstrip("0")


class Book:
    def __init__(self, library, depth=0):
        self.library = library
        self.handle = library.book_create(depth)
        if not self.handle:
            raise MemoryError("allocating order book")
        self.depth = depth
        self.ready = False
        self.status = "initializing"

    def close(self):
        self.library.book_destroy(self.handle)
        self.handle = None

    def invalidate(self, status="invalid"):
        self.library.book_reset(self.handle, False)
        self.ready = False
        self.status = status

    def apply(self, snapshot, changes, checksum=None):
        if snapshot:
            self.library.book_reset(self.handle, True)
        elif not self.ready:
            return
        try:
            for bid, price, quantity in changes:
                digits = (checksum_digits(price) + checksum_digits(quantity)).encode() if self.depth else b""
                if not self.library.book_set(self.handle, bid, fixed(price), fixed(quantity), digits):
                    raise ValueError("could not apply book level")
            actual = self.library.book_finish(self.handle)
            if self.depth and actual != checksum:
                raise ValueError("Kraken book checksum mismatch")
        except (ValueError, ArithmeticError):
            self.invalidate()
            raise
        self.ready = True
        self.status = "valid" if self.valid() else "invalid"

    def valid(self):
        return self.library.book_valid(self.handle)

    def levels(self, bid):
        levels = ct.POINTER(Level)()
        count = self.library.book_levels(self.handle, bid, ct.byref(levels))
        return [(levels[i].price / SCALE, levels[i].quantity / SCALE) for i in range(count)]

    def checkpoint(self):
        sides = []
        for bid in (False, True):
            levels = ct.POINTER(Level)()
            count = self.library.book_levels(self.handle, bid, ct.byref(levels))
            sides.append({
                "levels": [(levels[i].price, levels[i].quantity, levels[i].digits.decode())
                           for i in range(count)],
            })
        return dict(ready=self.ready, status=self.status, sides=sides)

    def restore(self, state):
        self.ready, self.status = state["ready"], state["status"]
        self.library.book_reset(self.handle, self.ready)
        for bid, side in enumerate(state["sides"]):
            for price, quantity, digits in side["levels"]:
                if not self.library.book_set(self.handle, bid, price, quantity, digits.encode()):
                    raise ValueError("invalid book checkpoint")
