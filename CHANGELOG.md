# Changelog

## v2.2.16 - 2021.12.02
- Update dependencies.
- Run tests against net6.0 tfm.

## v2.2.14 - 2021.03.14
- Update dependencies.
- Run tests against net5.0 tfm.

## v2.2.12 - 2021.02.17
- Implement distributed lock expiration signal (`IRemoteLock.LockAliveToken`) for more robust leader election.

## v2.2.8 - 2019.12.23
- Adjust root namespace name to match assembly name.

## v2.2.5 - 2019.11.15
- Use precise monotonic timestamp from [SkbKontur.Cassandra.TimeGuid](https://github.com/skbkontur/cassandra-time-guid) package.
- Use [SourceLink](https://github.com/dotnet/sourcelink) to help ReSharper decompiler show actual code.

## v2.1.8 - 2019.10.12
- Target .NET Standard 2.0 (PR [#1](https://github.com/skbkontur/cassandra-distributed-lock/pull/1)).

## v2.0 - 2018.09.12
- Use [SkbKontur.Cassandra.ThriftClient](https://github.com/skbkontur/cassandra-thrift-client) package from NuGet.
- Use [SkbKontur.Cassandra.Local](https://github.com/skbkontur/cassandra-local) module for integration testing.
- Switch to SDK-style project format and dotnet core build tooling.
- Set TargetFramework to net471.
- Use [Vostok.Logging.Abstractions](https://github.com/vostok/logging.abstractions) as a logging framework facade.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.
