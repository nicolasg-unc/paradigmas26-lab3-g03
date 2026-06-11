JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
export PATH := $(JAVA_HOME)/bin:$(PATH)
export SBT_OPTS := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

.PHONY: compile run run-local clean

# Compila el proyecto sin ejecutarlo
compile:
	sbt compile

# Compila y ejecuta el proyecto
run:
	sbt run

# Compila y ejecuta el proyecto usando el servidor mock local (requiere mock corriendo en localhost:8123)
run-local:
	sbt "run --subscription-file data/local_subscriptions.json"

# Elimina los archivos compilados
clean:
	sbt clean

