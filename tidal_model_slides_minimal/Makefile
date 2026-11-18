PYTHON ?= python
MAIN ?= ..

.PHONY: slides smoke-model validate-full check sync-from-main push-to-main init-git clean clean-outputs

slides:
	latexmk -pdf -interaction=nonstopmode tidal.tex

smoke-model:
	$(PYTHON) scripts/smoke_model.py

validate-full:
	$(PYTHON) scripts/validate_full_model.py

check:
	$(PYTHON) scripts/check_package.py

sync-from-main:
	$(PYTHON) scripts/sync_from_main.py --main "$(MAIN)" --prune

push-to-main:
	@test -n "$(FILE)" || (echo "usage: make push-to-main FILE='tidal.tex [more paths]'" && exit 1)
	$(PYTHON) scripts/push_to_main.py $(FILE)

init-git:
	git init
	git add .
	git commit -m "Initial tidal model slides package"

clean:
	latexmk -c tidal.tex
	rm -f tmp*.tmp

clean-outputs:
	rm -rf outputs/validation_* outputs/qa_render
