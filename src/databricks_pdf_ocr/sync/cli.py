"""CLI interface for PDF sync operations."""

import logging
from pathlib import Path

import click
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

from .pdf_sync import PDFSync, SyncConfig


def setup_logging(verbose: bool) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
def sync_cli() -> None:
    """PDF sync commands for Databricks volumes."""
    pass


@sync_cli.command()
@click.argument("local_path", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--catalog", required=True, help="Databricks catalog name")
@click.option("--schema", required=True, help="Databricks schema name")
@click.option("--volume", required=True, help="Databricks volume name")
@click.option(
    "--pattern", "-p", multiple=True, default=["*.pdf", "*.PDF"], help="File patterns to sync"
)
@click.option("--exclude", "-e", multiple=True, help="Patterns to exclude")
@click.option("--dry-run", is_flag=True, help="Show what would be uploaded without uploading")
@click.option("--force", is_flag=True, help="Force upload all files, ignoring existing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def upload(
    local_path: str,
    catalog: str,
    schema: str,
    volume: str,
    pattern: list[str],
    exclude: list[str],
    dry_run: bool,
    force: bool,
    verbose: bool,
) -> None:
    """Upload PDFs from local directory to Databricks volume."""
    setup_logging(verbose)

    config = SyncConfig(
        local_path=Path(local_path),
        volume_path=f"/Volumes/{catalog}/{schema}/{volume}",
        catalog=catalog,
        schema=schema,
        volume=volume,
        patterns=list(pattern),
        exclude_patterns=list(exclude),
        dry_run=dry_run,
        force_upload=force,
    )

    try:
        sync = PDFSync()
        result = sync.sync(config)

        # Print results
        click.echo("\nSync Summary:")
        click.echo(f"  Uploaded: {result.success_count} files")
        click.echo(f"  Skipped: {result.skip_count} files")
        click.echo(f"  Failed: {result.failure_count} files")
        click.echo(f"  Duration: {result.duration_seconds:.1f} seconds")

        if result.total_bytes_uploaded > 0:
            mb_uploaded = result.total_bytes_uploaded / (1024 * 1024)
            click.echo(f"  Data uploaded: {mb_uploaded:.2f} MB")

        if result.failed_files:
            click.echo("\nFailed uploads:")
            for file_path, error in result.failed_files:
                click.echo(f"  - {file_path}: {error}")

        if dry_run:
            click.echo("\nDRY RUN: No files were actually uploaded")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.ClickException(str(e)) from e


@sync_cli.command()
@click.option("--catalog", required=True, help="Databricks catalog name")
@click.option("--schema", required=True, help="Databricks schema name")
@click.option("--volume", required=True, help="Databricks volume name")
def list_remote(catalog: str, schema: str, volume: str) -> None:
    """List PDFs in Databricks volume."""
    setup_logging(False)

    try:
        client = WorkspaceClient()
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

        click.echo(f"Listing PDFs in {volume_path}:\n")

        pdf_count = 0
        total_size = 0

        for file_info in client.files.list_directory_contents(directory_path=volume_path):
            if file_info.path and file_info.path.lower().endswith(".pdf"):
                pdf_count += 1
                size_mb = file_info.file_size / (1024 * 1024) if file_info.file_size else 0
                total_size += file_info.file_size or 0

                click.echo(f"  {Path(file_info.path).name} ({size_mb:.2f} MB)")

        click.echo(f"\nTotal: {pdf_count} PDFs, {total_size / (1024 * 1024):.2f} MB")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.ClickException(str(e)) from e


@sync_cli.command()
@click.option("--catalog", required=True, help="Databricks catalog name")
@click.option("--schema", required=True, help="Databricks schema name")
@click.option("--volume", required=True, help="Databricks volume name")
def create_volume(catalog: str, schema: str, volume: str) -> None:
    """Create Databricks volume if it doesn't exist."""
    setup_logging(False)

    try:
        client = WorkspaceClient()

        # Check if schema exists, create if it doesn't
        try:
            client.schemas.get(f"{catalog}.{schema}")
            click.echo(f"Schema exists: {catalog}.{schema}")
        except Exception:
            click.echo(f"Schema does not exist, creating: {catalog}.{schema}")
            client.schemas.create(catalog_name=catalog, name=schema)
            click.echo("Schema created successfully")

        # Check if volume exists
        try:
            volume_info = client.volumes.read(f"{catalog}.{schema}.{volume}")
            click.echo(f"Volume already exists: {volume_info.full_name}")
            return
        except Exception:
            pass

        # Create volume
        click.echo(f"Creating volume: {catalog}.{schema}.{volume}")
        client.volumes.create(
            catalog_name=catalog, schema_name=schema, name=volume, volume_type=VolumeType.MANAGED
        )
        click.echo("Volume created successfully")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    sync_cli()
