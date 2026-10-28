.PHONY: build run test docker-build docker-run docker-push clean release docker-buildx docker-buildx-push docker-buildx-setup docker-validate docker-regression-test site-stage site-check site-serve site-jsonld site-motion

APP_NAME = malachi
DOCKER_USERNAME ?= hectorcardoso
VERSION ?= $(shell grep '@version' mix.exs | head -1 | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')
PLATFORMS ?= linux/amd64,linux/arm64

build:
	mix deps.get
	mix compile

run:
	mix run --no-halt

test:
	mix test

release:
	MIX_ENV=prod mix deps.get
	MIX_ENV=prod mix release

docker-build:
	docker build --build-arg APP_VERSION=$(VERSION) -t $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION) .
	docker tag $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION) $(DOCKER_USERNAME)/$(APP_NAME):latest

docker-buildx-setup:
	@echo "Setting up Docker Buildx..."
	docker buildx create --name $(APP_NAME)-builder --use --bootstrap || docker buildx use $(APP_NAME)-builder
	docker buildx inspect --bootstrap

docker-buildx:
	@echo "Building multi-platform Docker image ($(PLATFORMS))..."
	docker buildx build \
		--platform $(PLATFORMS) \
		--build-arg APP_VERSION=$(VERSION) \
		-t $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION) \
		-t $(DOCKER_USERNAME)/$(APP_NAME):latest \
		--load \
		.

docker-buildx-push:
	@echo "Building and pushing multi-platform Docker image ($(PLATFORMS))..."
	docker buildx build \
		--platform $(PLATFORMS) \
		--build-arg APP_VERSION=$(VERSION) \
		-t $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION) \
		-t $(DOCKER_USERNAME)/$(APP_NAME):latest \
		--push \
		.

docker-run:
	docker run \
		--name $(APP_NAME) \
		-p 4040:4040 \
		-p 4041:4041 \
		-e MALACHI_ADMIN_PASS="admin_dev_pass" \
		-e MALACHI_PRODUCER_PASS="producer_dev_pass" \
		-e MALACHI_CONSUMER_PASS="consumer_dev_pass" \
		-e MALACHI_APP_PASS="app_dev_pass" \
		$(DOCKER_USERNAME)/$(APP_NAME):$(VERSION)

docker-stop:
	docker stop $(APP_NAME) || true
	docker rm $(APP_NAME) || true

docker-push:
	docker push $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION)
	docker push $(DOCKER_USERNAME)/$(APP_NAME):latest

docker-logs:
	docker logs -f $(APP_NAME)

compose-up:
	docker-compose up -d

compose-down:
	docker-compose down

compose-logs:
	docker-compose logs -f

compose-producer:
	docker-compose --profile tools run --rm producer

docker-validate:
	@echo "Running Docker build validation..."
	./scripts/validate-docker-build.sh

docker-regression-test:
	@echo "Running Docker regression tests..."
	./scripts/docker-regression-test.sh $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION)

docker-test-all: docker-build docker-validate docker-regression-test
	@echo "All Docker tests completed successfully!"

clean:
	rm -rf _build deps
	docker rmi $(DOCKER_USERNAME)/$(APP_NAME):$(VERSION) || true
	docker rmi $(DOCKER_USERNAME)/$(APP_NAME):latest || true

all: build test docker-build

# Landing page. `mix docs` stages the page into doc/; these targets run the checks by hand. None of them measures
# performance: the Lighthouse and browser checks run on Linux in CI, not on a laptop.
# doc/llms.txt is this page's own summary here: ExDoc writes llms.txt only with the markdown formatter, which mix.exs
# does not enable. Removing it first lets staging write it again, instead of refusing a summary from an older build.
site-stage:
	rm -f doc/llms.txt
	mix docs --warnings-as-errors

site-check:
	mix malachi.docs.site --check

site-serve:
	@echo "serving doc/ at http://127.0.0.1:8080/ (Ctrl-C to stop)"
	python3 -m http.server 8080 --bind 127.0.0.1 --directory doc

site-jsonld:
	@sed -n 's/.*<script type="application\/ld+json">\(.*\)<\/script>.*/\1/p' doc/index.html > doc/jsonld.json
	@echo "wrote doc/jsonld.json: paste it at https://validator.schema.org"

site-motion:
	@echo "Reduced motion, by hand (run make site-serve first):"
	@echo "  1. Open http://127.0.0.1:8080/ in Chrome, with DevTools open (Cmd+Option+I)"
	@echo "  2. Cmd+Shift+P, type 'Emulate CSS prefers-reduced-motion', choose reduce"
	@echo "  3. Reload: every scene shows a still frame and does not move"
	@echo "  4. Turn the emulation off, click a scene, press Space: it pauses, and resumes on a second press"
	@echo "  5. Scroll a scene out of view and back: it stops while hidden and resumes"
