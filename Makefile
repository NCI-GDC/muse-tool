.PHONY: docker-*
docker-login:
	@echo
	docker login -u="${QUAY_USERNAME}" -p="${QUAY_PASSWORD}" quay.io

.PHONY: build build-*
build: build-muse build-multi-muse build-muse-merge

build-%:
	@echo
	@echo -- Building docker --
	@make -C $* build-docker NAME=$*

.PHONY: publish publish-% publish-release publish-release-%

publish: publish-muse publish-multi-muse publish-muse-merge

publish-%:
	@echo
	@make -C $* publish

publish-release:

publish-release-%:
	@echo
	@make -C $* publish-release

help: 
	@echo build
	@echo publish
