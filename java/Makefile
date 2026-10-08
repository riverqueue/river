.PHONY: test test/conformance test/postgres test/sqlite test/tools lint lint/tools format package
test:
	$(MAKE) -C .. test/java
test/conformance:
	$(MAKE) -C .. test/java/conformance
test/postgres:
	$(MAKE) -C .. test/java/postgres
test/sqlite:
	$(MAKE) -C .. test/java/sqlite
test/tools:
	$(MAKE) -C .. test/java/tools
lint:
	$(MAKE) -C .. lint/java
lint/tools:
	$(MAKE) -C .. lint/java/tools
format:
	mvn spotless:apply
package:
	$(MAKE) -C .. check/java/package
