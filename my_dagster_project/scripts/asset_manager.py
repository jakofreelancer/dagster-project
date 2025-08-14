import click
from my_dagster_project.core.metadata_store import metadata_store
from my_dagster_project.governance.dashboard import GovernanceDashboard
from my_dagster_project.core.asset_discovery import AssetDiscovery

@click.group()
def cli():
    """Enterprise Asset Management CLI"""
    pass

@cli.command()
def init():
    """Initialize the asset management system"""
    from my_dagster_project.scripts.initialize import initialize_asset_management
    try:
        initialize_asset_management()
        click.echo("Asset management system initialized successfully!")
    except Exception as e:
        click.echo(f"Failed to initialize asset management system: {e}")

@cli.command()
def list_assets():
    """List all registered assets"""
    assets = metadata_store.get_all_assets()
    
    if not assets:
        click.echo("No assets registered.")
        return
    
    click.echo(f"{'Asset Key':<30} {'Name':<25} {'Type':<15} {'Group':<20}")
    click.echo("-" * 90)
    
    for asset in assets:
        click.echo(f"{asset['asset_key']:<30} {asset['asset_name']:<25} {asset['asset_type']:<15} {asset['group_name']:<20}")

@cli.command()
def asset_health():
    """Show asset health status"""
    dashboard = GovernanceDashboard()
    health_df = dashboard.get_asset_health()
    
    if health_df.empty:
        click.echo("No asset health data available.")
        return
    
    click.echo(f"{'Asset Key':<30} {'Status':<15} {'Alerts':<10} {'Last Execution':<20}")
    click.echo("-" * 75)
    
    for _, row in health_df.iterrows():
        click.echo(f"{row['asset_key']:<30} {row['health_status']:<15} {row['alert_count']:<10} {str(row['last_execution_time'])[:19] if row['last_execution_time'] else 'N/A':<20}")

@cli.command()
@click.argument('asset_key')
def asset_details(asset_key):
    """Show details for a specific asset"""
    asset = metadata_store.get_asset(asset_key)
    
    if not asset:
        click.echo(f"Asset '{asset_key}' not found.")
        return
    
    click.echo(f"Asset Key: {asset['asset_key']}")
    click.echo(f"Name: {asset['asset_name']}")
    click.echo(f"Type: {asset['asset_type']}")
    click.echo(f"Group: {asset['group_name']}")
    click.echo(f"Pipeline: {asset['pipeline_name']}")
    click.echo(f"Owners: {', '.join(asset['owners'])}")
    click.echo(f"Created: {asset['created_at']}")
    click.echo(f"Updated: {asset['updated_at']}")
    
    if asset['tags']:
        click.echo("Tags:")
        for key, value in asset['tags'].items():
            click.echo(f"  {key}: {value}")
    
    if asset['metadata']:
        click.echo("Metadata:")
        for key, value in asset['metadata'].items():
            click.echo(f"  {key}: {value}")
    
    # Show lineage
    lineage = metadata_store.get_asset_lineage(asset_key)
    if lineage['upstream']:
        click.echo("Upstream Dependencies:")
        for dep in lineage['upstream']:
            click.echo(f"  <- {dep['asset_key']}")
    
    if lineage['downstream']:
        click.echo("Downstream Dependencies:")
        for dep in lineage['downstream']:
            click.echo(f"  -> {dep['asset_key']}")

@cli.command()
def discover_assets():
    """Discover and register assets"""
    discovery = AssetDiscovery()
    assets = discovery.discover_assets()
    
    click.echo(f"Discovered {len(assets)} assets:")
    for asset in assets:
        click.echo(f"  - {asset['module_name']}")

@cli.command()
def asset_inventory():
    """Generate a comprehensive asset inventory report"""
    dashboard = GovernanceDashboard()
    inventory_df = dashboard.get_asset_inventory()
    
    if inventory_df.empty:
        click.echo("No assets found.")
        return
    
    # Display summary
    click.echo(f"Total Assets: {len(inventory_df)}")
    click.echo(f"Asset Groups: {inventory_df['group_name'].nunique()}")
    click.echo(f"Owners: {inventory_df['owner_count'].sum()} (across all assets)")
    
    # Show asset types
    click.echo("\nAsset Types:")
    type_counts = inventory_df['asset_type'].value_counts()
    for asset_type, count in type_counts.items():
        click.echo(f"  {asset_type}: {count}")

@cli.command()
def ownership_report():
    """Generate asset ownership report"""
    dashboard = GovernanceDashboard()
    ownership_df = dashboard.get_ownership_report()
    
    if ownership_df.empty:
        click.echo("No ownership data available.")
        return
    
    click.echo(f"{'Owner':<30} {'Asset Count':<15}")
    click.echo("-" * 45)
    
    owner_counts = ownership_df['owner'].value_counts()
    for owner, count in owner_counts.items():
        click.echo(f"{owner:<30} {count:<15}")

if __name__ == '__main__':
    cli()