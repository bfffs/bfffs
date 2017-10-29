all: prime-s.pdf

prime-s.pdf: prime-s.dvi
	dvipdf $>

prime-s.dvi: prime-s.tex
	latex $>

clean:
	rm -f *.aux *.dvi *.log *.pdf
