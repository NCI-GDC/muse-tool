.PHONY: help
help: 
	@echo build - Create all docker images
	@echo publish - Push all latest docker images

.PHONY: docker-*
docker-login:
	@echo
	docker login -u="${QUAY_USERNAME}" -p="${QUAY_PASSWORD}" quay.io

.PHONY: build build-*
build: build-muse build-multi-muse build-merge-muse

build-%:
	@echo
	@echo -- Building docker --
	@make -C $* build-docker NAME=$*

.PHONY: publish-staging publish-staging-% publish-release publish-release-%

publish-staging: publish-staging-muse publish-staging-multi-muse publish-staging-merge-muse
publish-staging-%:
	@echo
	@make -C $* publish-staging

publish-release: publish-release-muse publish-release-multi-muse publish-merge-muse

publish-release-%:
	@echo
	@make -C $* publish-release

