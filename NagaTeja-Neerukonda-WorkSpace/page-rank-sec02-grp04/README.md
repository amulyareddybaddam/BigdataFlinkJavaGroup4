My README.md

#word-count-beam

Java Quickstart for Apache Beam

<https://beam.apache.org/get-started/quickstart-java>

## Set up Environment

- Java
- Maven

## Get the Sample Project

```PowerShell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false`
```

## Execute using DirectRunner

```PowerShell
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
```

## Execute PR Quick Start

```PowerShell
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.MinimalPageRankCase 
```

### Group-Repo Launch page
<https://github.com/amulyareddybaddam/BigdataFlinkJavaGroup4>


### My Wiki Link
<https://github.com/amulyareddybaddam/BigdataFlinkJavaGroup4/wiki/NagaTeja-Neerukonda>

### Link to MyCode folder
<https://github.com/amulyareddybaddam/BigdataFlinkJavaGroup4/tree/main/NagaTeja-Neerukonda-WorkSpace/page-rank-sec02-grp04/src/main/java/edu/nwmsu/grp4sec2/neerukonda>


I have completed the Job1 and Job1Finilizer till now, got some issues with the Job2Mapper method, working on it.