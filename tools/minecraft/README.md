# Fabric Minecraft Server – Probe & Snapshot Helper

This Windows batch script inspects a Fabric Minecraft server directory and produces
a deterministic report of server state. It can also stage a clean snapshot folder
suitable for backup with WCBT or any other backup tool.

## Usage

1. Stop the Minecraft server.
2. Place `wcbt_fabric_probe_and_stage.bat` in the server root
   (next to `server.properties`).
3. Run the script.
4. Review the generated `_wcbt_probe/` reports.
5. If prompted, allow creation of `_wcbt_snapshot/`.

The `_wcbt_snapshot/` directory represents a complete, restorable server state.

## Notes

- World folders are detected automatically by locating `level.dat`.
- Mods are hashed (SHA256) to provide an exact fingerprint.
- Logs and crash reports are excluded by default.
