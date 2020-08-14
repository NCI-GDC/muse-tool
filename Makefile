.PHONY: help
help: 
	@echo build - Create all docker images
	@echo publish - Push all latest docker images

.PHONY: docker-*
docker-login:
	@echo
	docker login -u="${QUAY_USERNAME}" -p="${QUAY_PASSWORD}" quay.io

.PHONY: build build-*
build: build-multi-muse build-merge-muse

build-%:
	@echo
	@echo -- Building docker --
	@make -C $* build-docker NAME=$*

.PHONY: publish-staging publish-staging-% publish-release publish-release-%

publish-staging: publish-staging-multi-muse publish-staging-merge-muse
publish-staging-%:
	@echo
	@make -C $* publish-staging

publish-release: publish-release-multi-muse publish-release-merge-muse

publish-release-%:
	@echo
	@make -C $* publish-release

