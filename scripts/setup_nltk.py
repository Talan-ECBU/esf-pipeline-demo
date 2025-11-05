# scripts/setup_nltk.py
"""
Script to set up NLTK resources.

Run this before processing reviews.
"""

import nltk

nltk.download("vader_lexicon")
nltk.download("punkt")
