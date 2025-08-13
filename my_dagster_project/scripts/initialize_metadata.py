"""Initialize metadata for existing assets"""

def initialize_metadata():
  """Register all assets in metadata system"""
  try:
    # Import and call registration functions
    from my_dagster_project.assets.ingestion.stg_blast_shotcrete_71_ingestion import register_asset_metadata as register_ingestion
    from my_dagster_project.assets.loaders.stg_blast_shotcrete_71_loader import register_asset_metadata as register_loader
    
    print("Registering ingestion asset...")
    register_ingestion()
    
    print("Registering loader asset...")
    register_loader()
    
    print("Metadata registration complete!")
      
  except Exception as e:
    print(f"Error during metadata registration: {e}")

if __name__ == "__main__":
  initialize_metadata()
