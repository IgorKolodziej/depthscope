/* Prices and quantities use exact 10^-8 units for source validation. */
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

typedef struct {
    int64_t price, quantity;
    char checksum_digits[42];
} Level;

typedef struct {
    Level *levels;
    size_t count, capacity;
} Side;

typedef struct {
    Side sides[2]; /* asks, bids */
    size_t depth; /* zero for the full Coinbase book */
    bool initialized;
} Book;

Book *book_create(size_t depth) {
    Book *book = calloc(1, sizeof(*book));
    if (book) book->depth = depth;
    return book;
}

void book_destroy(Book *book) {
    if (!book) return;
    free(book->sides[0].levels);
    free(book->sides[1].levels);
    free(book);
}

void book_reset(Book *book, bool initialized) {
    for (size_t i = 0; i < 2; ++i) {
        book->sides[i].count = 0;
    }
    book->initialized = initialized;
}

bool book_set(Book *book, bool bid, int64_t price, int64_t quantity, const char *digits) {
    if (!book->initialized || price <= 0 || quantity < 0) return false;
    Side *side = &book->sides[bid];
    size_t left = 0, right = side->count;
    while (left < right) {
        size_t middle = left + (right - left) / 2;
        int64_t other = side->levels[middle].price;
        if (bid ? other > price : other < price) left = middle + 1;
        else right = middle;
    }
    Level *levels = side->levels;
    bool exists = left < side->count && levels[left].price == price;
    if (quantity == 0) {
        if (exists) {
            memmove(levels + left, levels + left + 1, (--side->count - left) * sizeof(*levels));
        }
        return true;
    }

    size_t digit_count = strlen(digits);
    if (digit_count >= sizeof(levels[0].checksum_digits)) return false;
    if (!exists) {
        if (side->count == side->capacity) {
            size_t capacity = side->capacity ? side->capacity * 2 : 128;
            levels = realloc(levels, capacity * sizeof(*levels));
            if (!levels) return false;
            side->levels = levels;
            side->capacity = capacity;
        }
        memmove(levels + left + 1, levels + left, (side->count++ - left) * sizeof(*levels));
    }
    levels[left].price = price;
    levels[left].quantity = quantity;
    memcpy(levels[left].checksum_digits, digits, digit_count + 1);
    return true;
}

uint32_t book_finish(Book *book) {
    uLong checksum = crc32(0, Z_NULL, 0);
    if (!book->depth) return 0;
    for (size_t i = 0; i < 2; ++i) {
        Side *side = &book->sides[i];
        if (side->count >= book->depth) {
            side->count = book->depth;
        }
        for (size_t j = 0; j < side->count && j < 10; ++j) {
            const char *digits = side->levels[j].checksum_digits;
            checksum = crc32(checksum, (const Bytef *) digits, (uInt) strlen(digits));
        }
    }
    return (uint32_t) checksum;
}

bool book_valid(const Book *book) {
    const Side *asks = &book->sides[0], *bids = &book->sides[1];
    if (!book->initialized || !asks->count || !bids->count) return false;
    return asks->levels[0].price > bids->levels[0].price;
}

size_t book_levels(const Book *book, bool bid, const Level **levels) {
    *levels = book->sides[bid].levels;
    return book->sides[bid].count;
}
