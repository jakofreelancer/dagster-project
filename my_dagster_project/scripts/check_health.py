"""Check pipeline health"""
from governance.dashboard import GovernanceDashboard

dashboard = GovernanceDashboard()

# Get health report
report = dashboard.generate_health_report()

print(f"Pipeline Health Report - {report['report_time']}")
print(f"Total Assets: {report['total_assets']}")
print(f"Healthy: {report['healthy_assets']}")
print(f"Unhealthy: {report['unhealthy_assets']}")
print(f"Stale: {report['stale_assets']}")
print(f"Never Run: {report['never_run_assets']}")

# Show unhealthy assets
health_df = dashboard.get_pipeline_health()
unhealthy = health_df[health_df['health_status'] != 'HEALTHY']

if not unhealthy.empty:
  print("\nUnhealthy Assets:")
  for _, asset in unhealthy.iterrows():
    print(f"  - {asset['asset_key']}: {asset['health_status']}")
