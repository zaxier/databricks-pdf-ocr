"""CLI for syncing local files to a Databricks volume."""

import logging
from pathlib import Path

import click
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from volsync import SyncConfig as VolSyncConfig
from volsync import VolumeSync

from .config import SyncConfig as AppSyncConfig


def setup_logging(verbose: bool) -> None:
    """Set up logging for the sync tool."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.command()
@click.option(
    "--create-volume",
    is_flag=True,
    help="Create the volume if it does not exist.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be uploaded without actually uploading.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force upload all files, ignoring existing ones.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose logging for detailed output.",
)
def main(create_volume: bool, dry_run: bool, force: bool, verbose: bool) -> None:
    """Sync local PDF files to a Databricks volume based on settings.toml."""
    setup_logging(verbose)

    try:
        # Load configuration from settings.toml
        config = AppSyncConfig()

        # Optionally create the volume
        if create_volume:
            create_volume_if_not_exists(config)

        # Configure and run the sync
        sync_config = VolSyncConfig(
            local_path=Path(config.local_path),
            volume_path=f"/Volumes/{config.catalog}/{config.schema}/{config.volume}",
            catalog=config.catalog,
            schema=config.schema,
            volume=config.volume,
            patterns=config.patterns,
            exclude_patterns=config.exclude_patterns,
            dry_run=dry_run,
            force_upload=force,
        )

        syncer = VolumeSync()
        result = syncer.sync(sync_config)

        # Print summary
        click.echo("\nSync Summary:")
        click.echo(f"  - Uploaded: {result.success_count} files")
        click.echo(f"  - Skipped: {result.skip_count} files")
        click.echo(f"  - Failed: {result.failure_count} files")
        click.echo(f"  - Duration: {result.duration_seconds:.2f} seconds")

        if result.total_bytes_uploaded > 0:
            mb_uploaded = result.total_bytes_uploaded / (1024 * 1024)
            click.echo(f"  - Data Uploaded: {mb_uploaded:.2f} MB")

        if result.failed_files:
            click.echo("\nFailed Uploads:")
            for file_path, error in result.failed_files:
                click.echo(f"  - {file_path}: {error}")

        if dry_run:
            click.echo("\nNOTE: Dry run mode was enabled. No files were actually uploaded.")

    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)
        raise click.ClickException(str(e)) from e


def create_volume_if_not_exists(config: AppSyncConfig) -> None:
    """Create the configured volume if it doesn't already exist."""
    client = WorkspaceClient()

    # Ensure schema exists
    try:
        client.schemas.get(f"{config.catalog}.{config.schema}")
        logging.info(f"Schema '{config.catalog}.{config.schema}' already exists.")
    except Exception:
        logging.info(f"Schema '{config.catalog}.{config.schema}' not found, creating it.")
        client.schemas.create(name=config.schema, catalog_name=config.catalog)
        click.echo(f"Schema '{config.catalog}.{config.schema}' created.")

    # Check for volume
    try:
        client.volumes.read(f"{config.catalog}.{config.schema}.{config.volume}")
        logging.info(f"Volume '{config.volume}' already exists.")
    except Exception:
        logging.info(f"Volume '{config.volume}' not found, creating it.")
        client.volumes.create(
            catalog_name=config.catalog,
            schema_name=config.schema,
            name=config.volume,
            volume_type=VolumeType.MANAGED,
        )
        click.echo(f"Volume '{config.volume}' created successfully.")


if __name__ == "__main__":
    main()
