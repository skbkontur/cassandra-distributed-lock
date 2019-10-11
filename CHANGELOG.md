# Changelog

## v2.1.0 - 2019.10.11
- Target .NET Standard 2.0 (PR [#1](https://github.com/skbkontur/cassandra-distributed-lock/pull/1)).

## v2.0 - 2018.09.12
- Use [SkbKontur.Cassandra.ThriftClient](https://github.com/skbkontur/cassandra-thrift-client) package from NuGet.
- Use [SkbKontur.Cassandra.Local](https://github.com/skbkontur/cassandra-local) module for integration testing.
- Switch to SDK-style project format and dotnet core build tooling.
- Set TargetFramework to net471.
- Use [Vostok.Logging.Abstractions](https://github.com/vostok/logging.abstractions) as a logging framework facade.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.
