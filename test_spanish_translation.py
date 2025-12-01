"""
Quick test script for Spanish translation pipeline
Tests the adaptive model selection and GPT translation
"""

import os

# Set your OpenAI API key BEFORE importing the script
# Load from .env file or set manually for testing
from dotenv import load_dotenv
load_dotenv()
# os.environ["OPENAI_API_KEY"] = "your-key-here"  # Uncomment and add your key for local testing

# Now import after setting the env var
from scripts.spanish_translation_generator import choose_model, build_prompt, get_translation

# Test cases
test_words = [
    ("hello", "people & relationships", "very common", "a1"),
    ("serendipity", "qualities, states & attributes", "rare", "c1"),
    ("run away", "actions & events", "common", "b1"),
    ("artificial intelligence", "computing & software", "uncommon", "b2"),
]

print("=" * 60)
print("SPANISH TRANSLATION PIPELINE TEST")
print("=" * 60)

for word, cat1, freq, level in test_words:
    print(f"\n📝 Word: '{word}'")
    print(f"   Category: {cat1}")
    print(f"   Frequency: {freq}")
    print(f"   Level: {level}")
    
    # Test model selection
    model = choose_model(word, cat1, freq, level)
    print(f"   ✓ Selected model: {model}")
    
    if model == "SKIP":
        print(f"   → Translation: {word} (SKIPPED)")
        continue
    
    # Test prompt building
    prompt = build_prompt(word, cat1, "Spanish")
    print(f"   ✓ Prompt built")
    
    # Test translation
    try:
        translation = get_translation(prompt, model)
        print(f"   ✓ Translation: {translation}")
    except Exception as e:
        print(f"   ✗ Error: {e}")

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)
