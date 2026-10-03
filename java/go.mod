// This boundary keeps java/ out of River's Go module zip and package discovery.
// Go maintenance tools in bin/ are run and tested by file from the root workspace.
// This module is not part of go.work. Do not tag it:
// a java/vX.Y.Z tag would be interpreted as a version of this module.
module github.com/riverqueue/river/java
