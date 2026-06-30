"""
PCA-based statistical risk model.

Replicates the factor risk model from the CVXPY/PCA Risk Model Demo notebook.

The portfolio variance is modelled as:
    w^T (B P B^T + S) w
where:
    B  — factor loadings (asset × PC)
    P  — PC covariance matrix (diagonal, annualised)
    S  — idiosyncratic variance matrix (diagonal, annualised)

Usage:
    returns = prices.pct_change().dropna()
    model = build_risk_model(returns, num_factors=5)
    risk_var = portfolio_variance(weights, model)
"""

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


def fit_pca(returns: pd.DataFrame, num_factors: int) -> PCA:
    pca = PCA(n_components=num_factors, svd_solver="full")
    pca.fit(returns)
    return pca


def pc_betas(pca: PCA, asset_names: np.ndarray) -> pd.DataFrame:
    """Factor loadings: shape (n_assets, n_factors)."""
    return pd.DataFrame(
        pca.components_.T,
        index=asset_names,
        columns=np.arange(pca.n_components_),
    )


def pc_returns(pca: PCA, returns: pd.DataFrame) -> pd.DataFrame:
    """Factor returns: shape (n_dates, n_factors)."""
    return pd.DataFrame(
        pca.transform(returns.values),
        index=returns.index,
        columns=np.arange(pca.n_components_),
    )


def pc_cov_matrix(factor_returns: pd.DataFrame, ann_factor: int = 252) -> np.ndarray:
    """Diagonal covariance matrix of the PC factors (annualised)."""
    return np.diag(factor_returns.var(axis=0, ddof=1) * ann_factor)


def idiosyncratic_var_matrix(
    returns: pd.DataFrame,
    factor_returns: pd.DataFrame,
    betas: pd.DataFrame,
    ann_factor: int = 252,
) -> pd.DataFrame:
    """Diagonal idiosyncratic (residual) variance matrix (annualised)."""
    common = pd.DataFrame(
        np.dot(factor_returns, betas.T),
        index=returns.index,
        columns=returns.columns,
    )
    residuals = returns - common
    idio = np.diag(np.var(residuals.values, axis=0, ddof=0)) * ann_factor
    return pd.DataFrame(idio, index=returns.columns, columns=returns.columns)


def build_risk_model(
    returns: pd.DataFrame,
    num_factors: int = 5,
    ann_factor: int = 252,
) -> dict:
    """
    Fit PCA risk model on returns DataFrame (dates × assets).

    Returns a dict with:
        pca                    — fitted PCA object
        explained_variance     — array of variance ratios per factor
        betas                  — pd.DataFrame (n_assets × n_factors)
        factor_returns         — pd.DataFrame (n_dates × n_factors)
        factor_cov_matrix      — np.ndarray  (n_factors × n_factors), diagonal
        idiosyncratic_var_matrix — pd.DataFrame (n_assets × n_assets), diagonal
        idiosyncratic_var_vector — pd.DataFrame (n_assets × 1)
    """
    pca = fit_pca(returns, num_factors)
    betas = pc_betas(pca, returns.columns.values)
    factor_rets = pc_returns(pca, returns)
    factor_cov = pc_cov_matrix(factor_rets, ann_factor)
    idio_matrix = idiosyncratic_var_matrix(returns, factor_rets, betas, ann_factor)
    idio_vector = pd.DataFrame(np.diagonal(idio_matrix), index=returns.columns)

    return {
        "pca": pca,
        "explained_variance": pca.explained_variance_ratio_,
        "betas": betas,
        "factor_returns": factor_rets,
        "factor_cov_matrix": factor_cov,
        "idiosyncratic_var_matrix": idio_matrix,
        "idiosyncratic_var_vector": idio_vector,
    }


def portfolio_variance(
    weights: np.ndarray,
    risk_model: dict,
    asset_names: pd.Index | None = None,
) -> float:
    """
    Compute annualised portfolio variance:  w^T (B P B^T + S) w

    Parameters
    ----------
    weights     : 1-D array aligned to asset_names (or to risk_model assets if None)
    risk_model  : dict returned by build_risk_model
    asset_names : optional subset / reordering of assets
    """
    B = risk_model["betas"]
    P = risk_model["factor_cov_matrix"]
    S = risk_model["idiosyncratic_var_matrix"]

    if asset_names is not None:
        B = B.loc[asset_names]
        S = S.loc[asset_names, asset_names]

    B = B.values
    S = S.values
    cov = B @ P @ B.T + S
    return float(weights @ cov @ weights)
