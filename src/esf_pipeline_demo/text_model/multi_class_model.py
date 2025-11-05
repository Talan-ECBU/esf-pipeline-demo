# src/esf_pipeline/text_model/multi_class_model.py
"""
Module for training and evaluating a multi-class text classification model.

This module implements a model that predicts compliance likelihood based on
product features.
"""

from logging import getLogger

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

logger = getLogger(__name__)


def train(
    training_data: pd.DataFrame,
    x_columns: list[str],
    y_columns: list[str],
    intercept: bool = False,
) -> pd.DataFrame:
    """Train Logistic Regression model."""
    x_train = training_data[x_columns]
    y_train = training_data[y_columns].idxmax(axis=1)
    model = LogisticRegression(
        solver="lbfgs", max_iter=1000, random_state=42, fit_intercept=intercept
    )
    model.fit(x_train, y_train)

    return model


def evaluate_model_performance(
    model: LogisticRegression,
    training_data: pd.DataFrame,
    x_columns: list[str],
    y_columns: list[str],
    plot_confusion: bool = True,
):
    """Evaluate classification performance of the logistic model."""
    x_eval = training_data[x_columns]
    y_true = training_data[y_columns].idxmax(axis=1)
    y_pred = model.predict(x_eval)

    metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "macro_precision": precision_score(
            y_true, y_pred, average="macro", zero_division=0
        ),
        "macro_recall": recall_score(y_true, y_pred, average="macro", zero_division=0),
        "macro_f1": f1_score(y_true, y_pred, average="macro", zero_division=0),
    }

    print("=== Classification Report ===")
    print(classification_report(y_true, y_pred, digits=3))

    if plot_confusion:
        cm = confusion_matrix(y_true, y_pred, labels=model.classes_)
        sns.heatmap(
            cm,
            annot=True,
            fmt="d",
            cmap="Blues",
            xticklabels=model.classes_,
            yticklabels=model.classes_,
        )
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.title("Confusion Matrix")
        plt.tight_layout()
        plt.show()

    mismatched = training_data.copy()
    mismatched["true_class"] = y_true
    mismatched["predicted_class"] = y_pred
    mismatched = mismatched[mismatched["true_class"] != mismatched["predicted_class"]]
    return metrics, mismatched


def train_and_evaluate(
    training_data: pd.DataFrame,
    x_columns: list[str],
    y_columns: list[str],
    intercept: bool = False,
    plot_confusion: bool = True,
):
    """Train and evaluate the multi-class model."""
    model = train(training_data, x_columns, y_columns, intercept=intercept)
    metrics, mismatched = evaluate_model_performance(
        model,
        training_data,
        x_columns,
        y_columns,
        plot_confusion=plot_confusion,
    )
    return model, metrics, mismatched
