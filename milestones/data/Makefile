LATEXMK ?= latexmk
MMDC ?= mmdc
DOCUMENTS := DepthScope_Report DepthScope_Tests DepthScope_Presentation
PDFS := $(addsuffix .pdf,$(DOCUMENTS))

.PHONY: pdf diagram clean
pdf: $(PDFS)

DepthScope_Report.pdf: source/system-architecture.pdf
DepthScope_Presentation.pdf: source/architecture.pdf

%.pdf: source/%.tex source/article-style.tex source/timing.tex
	mkdir -p build
	$(LATEXMK) -pdf -interaction=nonstopmode -halt-on-error -outdir=build $<
	cp build/$@ $@

diagram:
	$(MMDC) -i source/architecture.mmd -o source/architecture.pdf
	$(MMDC) -i source/system-architecture.mmd -o source/system-architecture.pdf

clean:
	rm -rf build
