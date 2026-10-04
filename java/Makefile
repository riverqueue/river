.PHONY: test lint format package
test:
	mvn test
lint:
	mvn verify -DskipTests
format:
	mvn spotless:apply
package:
	mvn package -DskipTests
