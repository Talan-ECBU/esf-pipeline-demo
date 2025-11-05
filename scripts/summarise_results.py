# Script to summarise results from final predictions.

import pandas as pd

from esf_pipeline.config import LOCAL_MODEL_DIR, LOCAL_RESULTS_DIR


def result_summary():
    final_predictions = pd.read_csv(LOCAL_MODEL_DIR / "final_predictions.csv")
    final_predictions = final_predictions[~final_predictions["is_irrelevant"]]

    grouped_stats = []

    text_pred_threshold = 0.5
    final_predictions["text_non_compliant"] = (
        final_predictions["text_nc_prob"] > text_pred_threshold
    )

    final_predictions["socket_non_compliant"] = final_predictions[
        "Socket section/Aus-North America non-compliant"
    ].astype(bool) | final_predictions["Socket section/UK-German non-compliant"].astype(
        bool
    )

    for group, df in final_predictions.groupby("product_group"):
        total_count = len(df)

        non_compliant_count = df["final_non_compliant"].sum()
        ip_non_compliant_count = df["adjusted_ip_incompliance"].sum()
        recalled_count = df["recalled_flag"].sum()
        socket_non_compliant_count = df["socket_non_compliant"].sum()
        text_non_compliant_count = df["text_non_compliant"].sum()

        grouped_stats.append(
            {
                "product_group": group,
                "total_count": total_count,
                "non_compliant_count": non_compliant_count,
                "non_compliant_%": (non_compliant_count / total_count) * 100,
                "ip_non_compliant_count": ip_non_compliant_count,
                "ip_non_compliant_%": (ip_non_compliant_count / total_count) * 100,
                "recalled_count": recalled_count,
                "recalled_%": (recalled_count / total_count) * 100,
                "socket_non_compliant_count": socket_non_compliant_count,
                "socket_non_compliant_%": (socket_non_compliant_count / total_count)
                * 100,
                "text_non_compliant_count": text_non_compliant_count,
                "text_non_compliant_%": (text_non_compliant_count / total_count) * 100,
            }
        )

    grouped_stats_df = pd.DataFrame(grouped_stats)

    total_count = grouped_stats_df["total_count"].sum()
    total_non_compliant = grouped_stats_df["non_compliant_count"].sum()
    total_ip_non_compliant = grouped_stats_df["ip_non_compliant_count"].sum()
    total_recalled = grouped_stats_df["recalled_count"].sum()
    total_socket_non_compliant = grouped_stats_df["socket_non_compliant_count"].sum()
    total_text_non_compliant = grouped_stats_df["text_non_compliant_count"].sum()

    total_row = {
        "product_group": "TOTAL",
        "total_count": total_count,
        "non_compliant_count": total_non_compliant,
        "non_compliant_%": (total_non_compliant / total_count) * 100,
        "ip_non_compliant_count": total_ip_non_compliant,
        "ip_non_compliant_%": (total_ip_non_compliant / total_count) * 100,
        "recalled_count": total_recalled,
        "recalled_%": (total_recalled / total_count) * 100,
        "socket_non_compliant_count": total_socket_non_compliant,
        "socket_non_compliant_%": (total_socket_non_compliant / total_count) * 100,
        "text_non_compliant_count": total_text_non_compliant,
        "text_non_compliant_%": (total_text_non_compliant / total_count) * 100,
    }

    result_summary = pd.concat(
        [grouped_stats_df, pd.DataFrame([total_row])], ignore_index=True
    )
    result_summary.to_csv(LOCAL_RESULTS_DIR / "main_summary.csv", index=False)


def score_summary():
    final_predictions = pd.read_csv(LOCAL_MODEL_DIR / "final_predictions.csv")
    final_predictions = final_predictions[~final_predictions["is_irrelevant"]]

    score_cols = [
        "voltage_score",
        "amperage_score",
        "wattage_score",
        "adjusted_danger_score",
        "is_recall_brand",
    ]

    if "is_recall_brand" in final_predictions.columns:
        final_predictions["is_recall_brand"] = (
            final_predictions["is_recall_brand"].astype(bool).astype(int)
        )

    existing_score_cols = [c for c in score_cols if c in final_predictions.columns]

    agg_dict = {col: "mean" for col in existing_score_cols if col != "is_recall_brand"}
    if "is_recall_brand" in existing_score_cols:
        agg_dict["is_recall_brand"] = "sum"

    score_summary = (
        final_predictions.groupby("product_group")[existing_score_cols]
        .agg(agg_dict)
        .reset_index()
    )

    score_summary.to_csv(LOCAL_RESULTS_DIR / "score_summary.csv", index=False)


def product_summary():
    final_predictions = pd.read_csv(LOCAL_MODEL_DIR / "final_predictions.csv")
    final_predictions = final_predictions[~final_predictions["is_irrelevant"]]
    query_summary = (
        final_predictions.groupby("query")
        .agg(
            total_products=("product_id", "nunique"),
            non_compliant_products=("final_non_compliant", "sum"),
        )
        .reset_index()
    )
    query_summary["non_compliant_%"] = (
        query_summary["non_compliant_products"] / query_summary["total_products"]
    ) * 100
    query_summary = query_summary.sort_values(by="non_compliant_%", ascending=False)
    query_summary = query_summary[:30]
    query_summary.to_csv(LOCAL_RESULTS_DIR / "query_summary.csv", index=False)


def marketplace_summary():
    final_predictions = pd.read_csv(LOCAL_MODEL_DIR / "final_predictions.csv")
    final_predictions = final_predictions[~final_predictions["is_irrelevant"]]
    marketplace_summary = (
        final_predictions.groupby("marketplace")
        .agg(
            total_products=("product_id", "nunique"),
            non_compliant_products=("final_non_compliant", "sum"),
        )
        .reset_index()
    )
    marketplace_summary["non_compliant_%"] = (
        marketplace_summary["non_compliant_products"]
        / marketplace_summary["total_products"]
    ) * 100
    marketplace_summary = marketplace_summary.sort_values(
        by="non_compliant_%", ascending=False
    )
    marketplace_summary.to_csv(
        LOCAL_RESULTS_DIR / "marketplace_summary.csv", index=False
    )


def non_compliant_list():
    final_predictions = pd.read_csv(LOCAL_MODEL_DIR / "final_predictions.csv")
    final_predictions = final_predictions[~final_predictions["is_irrelevant"]]
    non_compliant_list = final_predictions[
        final_predictions["final_non_compliant"] == 1
    ]
    non_compliant_list = non_compliant_list[
        [
            "product_id",
            "marketplace",
            "product_group",
            "title",
            "seller_id",
            "url",
            "final_non_compliant",
            "voltage_info",
            "amperage_info",
            "wattage_info",
            "manufacturer",
            "adjusted_danger_score",
            "waterproof_flag",
            "ip_rating",
            "adjusted_ip_incompliance",
            "recalled_flag",
            "is_recall_brand",
            "Socket section/UK-German non-compliant",
            "Socket section/Aus-North America non-compliant",
        ]
    ]
    non_compliant_list.to_csv(LOCAL_RESULTS_DIR / "non_compliant_list.csv", index=False)


def main():
    result_summary()
    score_summary()
    product_summary()
    marketplace_summary()
    non_compliant_list()


if __name__ == "__main__":
    main()
