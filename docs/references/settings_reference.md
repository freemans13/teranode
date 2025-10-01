# Comprehensive Settings Reference

This document provides a complete reference for all Teranode configuration settings, organized by component.

## Table of Contents

1. [Overview](#overview)
2. [General Configuration](#general-configuration)
3. [Services](#services)
   - [Alert Service](#alert-service)
   - [Asset Server](#asset-server)
   - [Block Assembly](#block-assembly)
   - [Blockchain](#blockchain)
   - [Block Persister](#block-persister)
   - [Block Validation](#block-validation)
   - [Legacy](#legacy)
   - [P2P](#p2p)
   - [Propagation](#propagation)
   - [RPC](#rpc)
   - [Subtree Validation](#subtree-validation)
   - [UTXO Persister](#utxo-persister)
   - [Validator](#validator)
4. [Stores](#stores)
   - [UTXO Store](#utxo-store)
   - [Blob Store](#blob-store)
5. [Kafka](#kafka)

## Overview

All Teranode services accept settings through a centralized Settings object that allows local and remote servers to have their own specific configuration.

For general information on how the configuration system works, see the [Settings Overview](settings.md).

For deployment-specific information, see:

- [Developer Setup](../tutorials/developers/developerSetup.md)
- [Docker Configuration](../howto/miners/docker/minersHowToConfigureTheNode.md)
- [Kubernetes Configuration](../howto/miners/kubernetes/minersHowToConfigureTheNode.md)

---

## General Configuration

### Configuration Files

Settings are stored in two files:

- `settings.conf`: Global settings with sensible defaults for all environments
- `settings_local.conf`: Developer-specific and deployment-specific overrides (not in source control)

### Configuration System

The configuration system uses a layered approach with the following priority:

1. `SETTING_NAME.context_name`: Context-specific override (highest priority)
2. `SETTING_NAME.base`: General override
3. `SETTING_NAME`: Base setting (lowest priority)

### Environment Variables

Most settings can be configured via environment variables using the pattern:

```text
TERANODE_<SERVICE>_<SETTING_NAME>
```

---

## Services

### Alert Service

**Topic Reference**: [Alert Service](../topics/services/alert.md)

The Alert Service implements Bitcoin SV's alert system for UTXO management and peer control.
