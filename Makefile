IMAGE_PREFIX = twosixlabsdart
IMAGE_NAME = scala-kafka-consumer
IMG := $(IMAGE_PREFIX)/$(IMAGE_NAME)
TAG = latest

docker-build:
	sbt clean assembly
	docker build -t $(IMG):$(TAG) .

docker-push: docker-build
	docker push $(IMG):$(TAG)