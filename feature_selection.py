import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE, SelectKBest, mutual_info_classif
from sklearn.model_selection import train_test_split

def feature_selection(df, target_col, method="importance", n_features=10, model=None):
    """
    Selects features from a DataFrame using sklearn-based analytics.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing features and target.
        target_col (str): The column name of the target variable.
        method (str): The method for feature selection:
            - "importance": Use feature importance from a tree-based model.
            - "rfe": Use Recursive Feature Elimination.
            - "mutual_info": Use Mutual Information to select features.
        n_features (int): Number of top features to select.
        model (sklearn model): Custom model for "importance" or "rfe" methods. If None, RandomForest is used.

    Returns:
        pd.DataFrame: DataFrame containing only the selected features.
    """
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found in the DataFrame.")

    # Split features and target
    X = df.drop(columns=[target_col])
    y = df[target_col]

    # Split the data (optional, but useful for generalization)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    if method == "importance":
        # Use feature importance from a tree-based model
        if model is None:
            model = RandomForestClassifier(random_state=42)
        model.fit(X_train, y_train)
        importances = model.feature_importances_
        feature_importance_df = pd.DataFrame({"feature": X.columns, "importance": importances})
        top_features = feature_importance_df.nlargest(n_features, "importance")["feature"].tolist()

    elif method == "rfe":
        # Use Recursive Feature Elimination
        if model is None:
            model = RandomForestClassifier(random_state=42)
        rfe = RFE(estimator=model, n_features_to_select=n_features)
        rfe.fit(X_train, y_train)
        top_features = X.columns[rfe.support_].tolist()

    elif method == "mutual_info":
        # Use Mutual Information
        selector = SelectKBest(mutual_info_classif, k=n_features)
        selector.fit(X_train, y_train)
        top_features = X.columns[selector.get_support()].tolist()

    else:
        raise ValueError(f"Unknown method '{method}'. Choose from 'importance', 'rfe', or 'mutual_info'.")

    # Return DataFrame with selected features
    return df[top_features + [target_col]]

# Example usage:
# df_selected = feature_selection(df, target_col="target", method="importance", n_features=5)

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def evaluate_feature_correlations(df, target_col=None, method="pearson", threshold=0.5, display_plot=True):
    """
    Evaluates feature correlations and optionally plots a heatmap.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        target_col (str, optional): The column name of the target variable. If provided, correlations with the target are shown separately.
        method (str): The method for correlation ('pearson', 'spearman', or 'kendall').
        threshold (float): Correlation threshold for flagging high correlations.
        display_plot (bool): Whether to display a heatmap of correlations.

    Returns:
        pd.DataFrame: Correlation matrix with features and target (if provided).
    """
    # Compute correlation matrix
    corr_matrix = df.corr(method=method)

    # Display correlation heatmap
    if display_plot:
        plt.figure(figsize=(10, 8))
        sns.heatmap(
            corr_matrix,
            annot=True,
            fmt=".2f",
            cmap="coolwarm",
            cbar=True,
            square=True,
            linewidths=0.5,
            annot_kws={"size": 8}
        )
        plt.title(f"Feature Correlation Heatmap ({method.capitalize()})")
        plt.show()

    # Find highly correlated features
    high_corr = (corr_matrix.abs() >= threshold).stack()
    high_corr = high_corr[high_corr & (high_corr.index.get_level_values(0) != high_corr.index.get_level_values(1))]
    high_corr_pairs = high_corr.index.tolist()

    print(f"Highly correlated pairs (|correlation| >= {threshold}):")
    for pair in high_corr_pairs:
        print(f"  {pair[0]} ↔ {pair[1]}: {corr_matrix.loc[pair[0], pair[1]]:.2f}")

    # Correlation with the target variable
    if target_col and target_col in corr_matrix.columns:
        target_corr = corr_matrix[target_col].drop(target_col)
        print(f"\nCorrelations with the target variable '{target_col}':")
        print(target_corr.sort_values(ascending=False))
        return corr_matrix, target_corr

    return corr_matrix



# FEATURE SELECTION EXAMPLE CODE
# Sample DataFrame
data = {
    "feature1": np.random.rand(100),
    "feature2": np.random.rand(100),
    "feature3": np.random.rand(100),
    "feature4": np.random.rand(100),
    "target": np.random.randint(0, 2, 100),
}
df = pd.DataFrame(data)

# Feature Selection Example
df_selected = feature_selection(df, target_col="target", method="importance", n_features=2)
print(df_selected.head())




# CORRELATION MATRIX EXAMPLE CODE
# Sample DataFrame
data = {
    "feature1": [1, 2, 3, 4, 5],
    "feature2": [5, 4, 3, 2, 1],
    "feature3": [1, 3, 5, 7, 9],
    "target": [0, 1, 0, 1, 0],
}
df = pd.DataFrame(data)

# Evaluate feature correlations
corr_matrix, target_corr = evaluate_feature_correlations(
    df, target_col="target", method="pearson", threshold=0.8, display_plot=True
)

# Inspect the correlation matrix
print("\nCorrelation Matrix:")
print(corr_matrix)