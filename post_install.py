"""
Post-installation script for Render deployment
Downloads required spaCy models and NLTK data
"""
import subprocess
import sys

def main():
    print("=" * 60)
    print("POST-INSTALL: Downloading NLP models and data...")
    print("=" * 60)
    
    # Download spaCy English model
    print("\n[1/2] Downloading spaCy en_core_web_sm model...")
    try:
        subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
        print("✅ spaCy model downloaded successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not download spaCy model: {e}")
    
    # Download NLTK data
    print("\n[2/2] Downloading NLTK WordNet data...")
    try:
        import nltk
        nltk.download('wordnet', quiet=True)
        nltk.download('omw-1.4', quiet=True)
        print("✅ NLTK data downloaded successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not download NLTK data: {e}")
    
    print("\n" + "=" * 60)
    print("POST-INSTALL COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
