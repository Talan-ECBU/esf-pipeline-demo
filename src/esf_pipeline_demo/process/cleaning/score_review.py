"""Module to process review data using Polars for performance."""

from functools import partial
from logging import getLogger

import numpy as np
import polars as pl
from nltk.sentiment import SentimentIntensityAnalyzer
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity

logger = getLogger(__name__)

DANGER_KEYWORDS = [
    "Defect",
    "Non-compliant",
    "Danger",
    "Injured",
    "Overheat",
    "Fire",
    "Hot",
    "Spark",
    "Burn",
    "Melt",
    "Exploded",
    "Arcing",
    "Arc",
    "Shock",
    "Electrocution",
    "Electric Shock",
    "Electrocute",
    "Short Circuit",
    "Smoke",
    "Toxic",
    "Harmful",
]

# Pre-load SentenceTransformer model globally
model = SentenceTransformer("all-MiniLM-L6-v2")


def provide_feedback_scores(df: pl.DataFrame, feedback_col: str) -> pl.DataFrame:
    """
    Add negativity and danger scores based on review text.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing review data.
    feedback_col : str
        The column containing feedback text.

    Returns
    -------
    pl.DataFrame
        DataFrame with 'review_text', 'negativity_score', and 'danger_score' columns.
    """
    sia = SentimentIntensityAnalyzer()

    if feedback_col not in df.columns:
        logger.warning(f"DataFrame does not contain '{feedback_col}' column.")
        return df.with_columns(
            [
                pl.lit("").alias("review_text"),
                pl.lit(0.0).alias("negativity_score"),
                pl.lit(0.0).alias("danger_score"),
            ]
        )

    # Fill nulls and cast to string
    df = df.with_columns(
        pl.col(feedback_col).fill_null("").cast(pl.Utf8).alias("review_text")
    )

    # Compute negativity score via SentimentIntensityAnalyzer
    negative_score_func = partial(_provide_negative_score, sia=sia)
    neg_scores = df["review_text"].to_list()
    neg_scores = [negative_score_func(text) for text in neg_scores]

    df = df.with_columns(pl.Series("negativity_score", neg_scores))

    # Compute semantic similarity danger score in batch
    df = df.with_columns(
        pl.Series(
            "danger_score",
            _batch_semantic_similarity(df["review_text"], DANGER_KEYWORDS),
        )
    )

    return df


def semantic_similarity(
    text: str,
    keywords: list[str],
    keyword_weights: dict[str, float] | None = None,
    normalise_length: bool = True,
) -> float:
    """
    Calculate semantic similarity between text and keywords, normalised by text length.
    """
    if not text or not keywords:
        return 0.0

    text_emb = model.encode(text, normalize_embeddings=True)
    keyword_embs = model.encode(keywords, normalize_embeddings=True)

    sims = util.cos_sim(text_emb, keyword_embs)[0].cpu().numpy()

    if keyword_weights:
        weights = np.array([keyword_weights.get(k, 1.0) for k in keywords])
        weighted_score = np.average(sims, weights=weights)
    else:
        weighted_score = np.mean(sims)

    if normalise_length:
        word_count = max(len(text.split()), 1)
        weighted_score /= np.log1p(word_count)

    return float(np.clip(weighted_score, 0.0, 1.0))


def _provide_negative_score(review_text: str, sia: SentimentIntensityAnalyzer) -> float:
    """Compute negativity intensity score using NLTK sentiment analyzer."""
    if not isinstance(review_text, str) or not review_text.strip():
        return 0.0
    scores = sia.polarity_scores(review_text)
    return float(scores.get("neg", 0.0))


def _batch_semantic_similarity(texts: pl.Series, keywords: list[str]) -> np.ndarray:
    """
    Compute semantic similarity between multiple texts and keywords in batch.
    """
    texts_list = texts.to_list()

    # Encode in batch for performance
    text_embs = model.encode(texts_list, batch_size=32, show_progress_bar=True)
    keyword_embs = model.encode(keywords)

    similarities = cosine_similarity(text_embs, keyword_embs)
    max_sims = np.max(similarities, axis=1)

    # Length-normalisation to avoid short-review bias
    word_counts = np.array([max(len(t.split()), 1) for t in texts_list])
    normalised_scores = max_sims / word_counts

    return normalised_scores.astype(float)
