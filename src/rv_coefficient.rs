// MIT License
//
// Copyright (c) 2026 Raja Lehtihet & Wael El Oraiby
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

//! Streaming RV coefficient for multivariate vector correlation.
//!
//! The RV coefficient ([Robert & Escoufier 1976]) is a multivariate generalization
//! of the squared Pearson correlation coefficient ($R^2$) between two sets of
//! random variables $X \in \mathbb{R}^p$ and $Y \in \mathbb{R}^q$.
//!
//! Given paired observations $(x_1, y_1), \dots, (x_n, y_n)$, let $\Sigma_{XX} \in \mathbb{R}^{p \times p}$
//! and $\Sigma_{YY} \in \mathbb{R}^{q \times q}$ be the sample covariance matrices of $X$ and $Y$,
//! and let $\Sigma_{XY} \in \mathbb{R}^{p \times q}$ be the sample cross-covariance matrix.
//! The RV coefficient is defined as:
//!
//! $$RV(X, Y) = \frac{\operatorname{tr}(\Sigma_{XY} \Sigma_{YX})}{\sqrt{\operatorname{tr}(\Sigma_{XX}^2) \operatorname{tr}(\Sigma_{YY}^2)}} = \frac{\|\Sigma_{XY}\|_F^2}{\|\Sigma_{XX}\|_F \|\Sigma_{YY}\|_F}$$
//!
//! where $\|\cdot\|_F$ denotes the Frobenius norm ($\|A\|_F^2 = \sum_{i,j} A_{ij}^2$).
//!
//! # Properties
//!
//! - **Bounded range**: $0 \le RV(X, Y) \le 1$.
//! - **Univariate reduction**: When $p = 1$ and $q = 1$, $RV(X, Y) = r^2$, the squared Pearson correlation coefficient.
//! - **Transformation invariance**: Invariant to scaling and orthogonal transformations (rotations/reflections) of $X$ and $Y$.
//! - **Zero correlation**: $RV(X, Y) = 0$ if and only if every coordinate of $X$ is uncorrelated with every coordinate of $Y$ ($\Sigma_{XY} = 0$).
//! - **Collinearity**: $RV(X, Y) = 1$ when $Y$ is an orthogonal transformation of $X$ (up to positive scaling).
//!
//! # Streaming representation and complexity
//!
//! [`RvCoefficient`] tracks the running means and centered cross-product sum matrices
//! $S_{XX}$, $S_{YY}$, and $S_{XY}$ in an online manner without storing historical observations.
//! Addition and batch merging use Pébay's pairwise multivariate Welford formulas ([SAND2008-6212]).
//!
//! Because sample scaling $\frac{1}{n-1}$ cancels out identically in the ratio of Frobenius norms,
//! $RV$ is evaluated directly from the unscaled centered sums:
//!
//! $$RV(X, Y) = \frac{\|S_{XY}\|_F^2}{\|S_{XX}\|_F \|S_{YY}\|_F}$$
//!
//! This requires $O(p^2 + q^2 + pq)$ time and $O(1)$ stack workspace, with **zero matrix multiplications**
//! and **zero heap allocations** during queries. Total retained memory is $O(p^2 + q^2 + pq)$ floats.
//!
//! [Robert & Escoufier 1976]: https://www.jstor.org/stable/2347233
//! [SAND2008-6212]: https://digital.library.unt.edu/ark:/67531/metadc837537/m2/1/high_res_d/1028931.pdf#page=13

use crate::SketchError;

/// Online streaming accumulator for the RV coefficient between two vector streams.
///
/// # Example
///
/// ```rust
/// use sketches::rv_coefficient::RvCoefficient;
///
/// // X has 2 features, Y has 2 features.
/// let mut rv = RvCoefficient::new(2, 2).unwrap();
///
/// // Perfectly correlated: Y is a scaled rotation of X.
/// rv.add(&[1.0, 0.0], &[0.0, 2.0]).unwrap();
/// rv.add(&[0.0, 1.0], &[-2.0, 0.0]).unwrap();
/// rv.add(&[-1.0, 0.0], &[0.0, -2.0]).unwrap();
/// rv.add(&[0.0, -1.0], &[2.0, 0.0]).unwrap();
///
/// let coeff = rv.rv_coefficient().unwrap();
/// assert!((coeff - 1.0).abs() < 1e-10);
/// ```
#[derive(Debug, Clone)]
pub struct RvCoefficient {
    /// Number of features in vector X; always positive.
    p: usize,
    /// Number of features in vector Y; always positive.
    q: usize,
    /// Exact observation count, checked before any update mutates the state.
    count: u64,
    /// Owned coordinate means for X; zeroed and unavailable when count is zero.
    mean_x: Vec<f64>,
    /// Owned coordinate means for Y; zeroed and unavailable when count is zero.
    mean_y: Vec<f64>,
    /// Centered sum of cross-products for X, row-major p x p with mirrored triangles.
    s_xx: Vec<f64>,
    /// Centered sum of cross-products for Y, row-major q x q with mirrored triangles.
    s_yy: Vec<f64>,
    /// Centered sum of cross-products between X and Y, row-major p x q.
    s_xy: Vec<f64>,
}

impl RvCoefficient {
    /// Creates an empty accumulator for paired vectors with $p$ and $q$ coordinates.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] if $p$ or $q$ is zero, if any matrix
    /// capacity overflows `usize`, or if storage allocation fails.
    pub fn new(p: usize, q: usize) -> Result<Self, SketchError> {
        if p == 0 {
            return Err(SketchError::InvalidParameter(
                "p dimension must be greater than zero",
            ));
        }
        if q == 0 {
            return Err(SketchError::InvalidParameter(
                "q dimension must be greater than zero",
            ));
        }

        let cells_xx = p
            .checked_mul(p)
            .ok_or(SketchError::InvalidParameter("p * p overflows usize"))?;
        let cells_yy = q
            .checked_mul(q)
            .ok_or(SketchError::InvalidParameter("q * q overflows usize"))?;
        let cells_xy = p
            .checked_mul(q)
            .ok_or(SketchError::InvalidParameter("p * q overflows usize"))?;

        let mut mean_x = Vec::new();
        mean_x
            .try_reserve_exact(p)
            .map_err(|_| SketchError::InvalidParameter("mean_x vector is too large to allocate"))?;
        mean_x.resize(p, 0.0);

        let mut mean_y = Vec::new();
        mean_y
            .try_reserve_exact(q)
            .map_err(|_| SketchError::InvalidParameter("mean_y vector is too large to allocate"))?;
        mean_y.resize(q, 0.0);

        let mut s_xx = Vec::new();
        s_xx.try_reserve_exact(cells_xx)
            .map_err(|_| SketchError::InvalidParameter("s_xx matrix is too large to allocate"))?;
        s_xx.resize(cells_xx, 0.0);

        let mut s_yy = Vec::new();
        s_yy.try_reserve_exact(cells_yy)
            .map_err(|_| SketchError::InvalidParameter("s_yy matrix is too large to allocate"))?;
        s_yy.resize(cells_yy, 0.0);

        let mut s_xy = Vec::new();
        s_xy.try_reserve_exact(cells_xy)
            .map_err(|_| SketchError::InvalidParameter("s_xy matrix is too large to allocate"))?;
        s_xy.resize(cells_xy, 0.0);

        Ok(Self {
            p,
            q,
            count: 0,
            mean_x,
            mean_y,
            s_xx,
            s_yy,
            s_xy,
        })
    }

    /// Creates an empty accumulator for paired vectors where both have the same dimension $k$.
    ///
    /// This is equivalent to [`Self::new(dimension, dimension)`].
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] if `dimension` is zero, if matrix
    /// dimensions overflow `usize`, or if storage allocation fails.
    pub fn new_equal(dimension: usize) -> Result<Self, SketchError> {
        Self::new(dimension, dimension)
    }

    /// Returns the number of features in vector X.
    pub fn p(&self) -> usize {
        self.p
    }

    /// Returns the number of features in vector Y.
    pub fn q(&self) -> usize {
        self.q
    }

    /// Returns the number of paired observations added so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns whether no observations have been added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the coordinate means of X, or `None` if the stream is empty.
    pub fn mean_x(&self) -> Option<&[f64]> {
        (self.count > 0).then_some(&self.mean_x)
    }

    /// Returns the coordinate means of Y, or `None` if the stream is empty.
    pub fn mean_y(&self) -> Option<&[f64]> {
        (self.count > 0).then_some(&self.mean_y)
    }

    /// Adds one finite observation pair $(x, y)$.
    ///
    /// A rejected observation leaves the accumulator state intact.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] if slice lengths do not match $p$ and $q$,
    /// or if any coordinate is not finite (NaN or infinite).
    /// Returns [`SketchError::ObservationCountOverflow`] if adding would exceed `u64::MAX`.
    pub fn add(&mut self, x: &[f64], y: &[f64]) -> Result<(), SketchError> {
        if x.len() != self.p {
            return Err(SketchError::InvalidParameter(
                "x vector dimension must match p",
            ));
        }
        if y.len() != self.q {
            return Err(SketchError::InvalidParameter(
                "y vector dimension must match q",
            ));
        }
        if x.iter().any(|v| !v.is_finite()) || y.iter().any(|v| !v.is_finite()) {
            return Err(SketchError::InvalidParameter(
                "vector coordinates must be finite",
            ));
        }

        let next_count = self
            .count
            .checked_add(1)
            .ok_or(SketchError::ObservationCountOverflow)?;

        if self.count == 0 {
            self.mean_x.copy_from_slice(x);
            self.mean_y.copy_from_slice(y);
            self.count = next_count;
            return Ok(());
        }

        let delta_x: Vec<f64> = x
            .iter()
            .zip(&self.mean_x)
            .map(|(&val, &mean)| val - mean)
            .collect();
        let delta_y: Vec<f64> = y
            .iter()
            .zip(&self.mean_y)
            .map(|(&val, &mean)| val - mean)
            .collect();

        let n = next_count as f64;
        let correction = self.count as f64 / n;

        // Update running means
        for (mean, diff) in self.mean_x.iter_mut().zip(&delta_x) {
            *mean += diff / n;
        }
        for (mean, diff) in self.mean_y.iter_mut().zip(&delta_y) {
            *mean += diff / n;
        }

        // Update S_xx (p x p, symmetric)
        let p = self.p;
        for i in 0..p {
            for j in i..p {
                let cross = weighted_cross_product(delta_x[i], delta_x[j], correction);
                self.s_xx[i * p + j] += cross;
                if i != j {
                    self.s_xx[j * p + i] = self.s_xx[i * p + j];
                }
            }
        }

        // Update S_yy (q x q, symmetric)
        let q = self.q;
        for i in 0..q {
            for j in i..q {
                let cross = weighted_cross_product(delta_y[i], delta_y[j], correction);
                self.s_yy[i * q + j] += cross;
                if i != j {
                    self.s_yy[j * q + i] = self.s_yy[i * q + j];
                }
            }
        }

        // Update S_xy (p x q, rectangular)
        for (i, &dx) in delta_x.iter().enumerate() {
            for (j, &dy) in delta_y.iter().enumerate() {
                let cross = weighted_cross_product(dx, dy, correction);
                self.s_xy[i * q + j] += cross;
            }
        }

        self.count = next_count;
        Ok(())
    }

    /// Combines independent batches with the same feature dimensions.
    ///
    /// A failed merge leaves the receiver unchanged.
    ///
    /// Updates are combined using Pébay's pairwise multivariate formulas ([SAND2008-6212]).
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSketches`] if $p$ or $q$ dimensions differ.
    /// Returns [`SketchError::ObservationCountOverflow`] if the combined count exceeds `u64::MAX`.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.p != other.p || self.q != other.q {
            return Err(SketchError::IncompatibleSketches(
                "vector dimensions p and q must match for merge",
            ));
        }

        let total = self
            .count
            .checked_add(other.count)
            .ok_or(SketchError::ObservationCountOverflow)?;

        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            self.mean_x.clone_from(&other.mean_x);
            self.mean_y.clone_from(&other.mean_y);
            self.s_xx.clone_from(&other.s_xx);
            self.s_yy.clone_from(&other.s_yy);
            self.s_xy.clone_from(&other.s_xy);
            self.count = other.count;
            return Ok(());
        }

        let delta_x: Vec<f64> = other
            .mean_x
            .iter()
            .zip(&self.mean_x)
            .map(|(&b, &a)| b - a)
            .collect();
        let delta_y: Vec<f64> = other
            .mean_y
            .iter()
            .zip(&self.mean_y)
            .map(|(&b, &a)| b - a)
            .collect();

        let weight = other.count as f64 / total as f64;
        let correction = (self.count as f64 * other.count as f64) / total as f64;

        for (mean, diff) in self.mean_x.iter_mut().zip(&delta_x) {
            *mean += diff * weight;
        }
        for (mean, diff) in self.mean_y.iter_mut().zip(&delta_y) {
            *mean += diff * weight;
        }

        let p = self.p;
        for i in 0..p {
            for j in i..p {
                let idx = i * p + j;
                let cross = weighted_cross_product(delta_x[i], delta_x[j], correction);
                self.s_xx[idx] += other.s_xx[idx] + cross;
                if i != j {
                    self.s_xx[j * p + i] = self.s_xx[idx];
                }
            }
        }

        let q = self.q;
        for i in 0..q {
            for j in i..q {
                let idx = i * q + j;
                let cross = weighted_cross_product(delta_y[i], delta_y[j], correction);
                self.s_yy[idx] += other.s_yy[idx] + cross;
                if i != j {
                    self.s_yy[j * q + i] = self.s_yy[idx];
                }
            }
        }

        for (i, &dx) in delta_x.iter().enumerate() {
            for (j, &dy) in delta_y.iter().enumerate() {
                let idx = i * q + j;
                let cross = weighted_cross_product(dx, dy, correction);
                self.s_xy[idx] += other.s_xy[idx] + cross;
            }
        }

        self.count = total;
        Ok(())
    }

    /// Computes the RV coefficient between vector streams X and Y.
    ///
    /// Evaluates:
    ///
    /// $$RV(X, Y) = \frac{\|S_{XY}\|_F^2}{\|S_{XX}\|_F \|S_{YY}\|_F}$$
    ///
    /// Returns `None` if fewer than two observations have been added (`count < 2`)
    /// or if either $X$ or $Y$ has zero variance (zero Frobenius norm), making
    /// the correlation mathematically undefined.
    ///
    /// The returned value is clamped to $[0.0, 1.0]$.
    pub fn rv_coefficient(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let mut norm_xx_sq = 0.0;
        for &val in &self.s_xx {
            norm_xx_sq += val * val;
        }

        let mut norm_yy_sq = 0.0;
        for &val in &self.s_yy {
            norm_yy_sq += val * val;
        }

        let mut norm_xy_sq = 0.0;
        for &val in &self.s_xy {
            norm_xy_sq += val * val;
        }

        let denom = (norm_xx_sq * norm_yy_sq).sqrt();
        if !denom.is_finite() || denom == 0.0 {
            return None;
        }

        Some((norm_xy_sq / denom).clamp(0.0, 1.0))
    }

    /// Computes the modified/adjusted RV coefficient ($RV_2$) ([Smilde et al. 2009]).
    ///
    /// In high-dimensional settings where $p$ or $q$ is large relative to sample size,
    /// standard RV can be inflated toward 1.0 due to the diagonal auto-variance elements.
    /// The modified RV coefficient excludes the diagonal elements of $S_{XX}$ and $S_{YY}$:
    ///
    /// $$RV_{\text{adj}} = \frac{\|S_{XY}\|_F^2}{\sqrt{\sum_{i \ne j} S_{XX}(i, j)^2 \sum_{k \ne l} S_{YY}(k, l)^2}}$$
    ///
    /// Returns `None` if $p < 2$, $q < 2$, `count < 2`, or if off-diagonal variances sum to zero.
    ///
    /// [Smilde et al. 2009]: https://doi.org/10.1093/bioinformatics/btp134
    pub fn adjusted_rv_coefficient(&self) -> Option<f64> {
        if self.count < 2 || self.p < 2 || self.q < 2 {
            return None;
        }

        let p = self.p;
        let mut norm_xx_off_sq = 0.0;
        for i in 0..p {
            for j in 0..p {
                if i != j {
                    let v = self.s_xx[i * p + j];
                    norm_xx_off_sq += v * v;
                }
            }
        }

        let q = self.q;
        let mut norm_yy_off_sq = 0.0;
        for i in 0..q {
            for j in 0..q {
                if i != j {
                    let v = self.s_yy[i * q + j];
                    norm_yy_off_sq += v * v;
                }
            }
        }

        let mut norm_xy_sq = 0.0;
        for &val in &self.s_xy {
            norm_xy_sq += val * val;
        }

        let denom = (norm_xx_off_sq * norm_yy_off_sq).sqrt();
        if !denom.is_finite() || denom == 0.0 {
            return None;
        }

        Some((norm_xy_sq / denom).clamp(0.0, 1.0))
    }

    /// Returns sample covariance matrix of X divided by $n - 1$, or `None` if $n < 2$.
    pub fn covariance_xx(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_xx, self.p, self.p, self.count.saturating_sub(1))
    }

    /// Returns sample covariance matrix of Y divided by $n - 1$, or `None` if $n < 2$.
    pub fn covariance_yy(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_yy, self.q, self.q, self.count.saturating_sub(1))
    }

    /// Returns sample cross-covariance matrix between X and Y divided by $n - 1$, or `None` if $n < 2$.
    pub fn covariance_xy(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_xy, self.p, self.q, self.count.saturating_sub(1))
    }

    /// Returns population covariance matrix of X divided by $n$, or `None` if empty.
    pub fn population_covariance_xx(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_xx, self.p, self.p, self.count)
    }

    /// Returns population covariance matrix of Y divided by $n$, or `None` if empty.
    pub fn population_covariance_yy(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_yy, self.q, self.q, self.count)
    }

    /// Returns population cross-covariance matrix between X and Y divided by $n$, or `None` if empty.
    pub fn population_covariance_xy(&self) -> Option<Vec<Vec<f64>>> {
        self.matrix_divided_by(&self.s_xy, self.p, self.q, self.count)
    }

    /// Removes all observations while retaining the configured dimensions and allocations.
    pub fn clear(&mut self) {
        self.count = 0;
        self.mean_x.fill(0.0);
        self.mean_y.fill(0.0);
        self.s_xx.fill(0.0);
        self.s_yy.fill(0.0);
        self.s_xy.fill(0.0);
    }

    fn matrix_divided_by(
        &self,
        matrix: &[f64],
        rows: usize,
        cols: usize,
        denominator: u64,
    ) -> Option<Vec<Vec<f64>>> {
        (denominator > 0).then(|| {
            let denom = denominator as f64;
            (0..rows)
                .map(|i| (0..cols).map(|j| matrix[i * cols + j] / denom).collect())
                .collect()
        })
    }
}

/// Symmetrically scales a product of deviations by a Welford weighting term.
///
/// Multiplies the larger absolute value by the weight first when weight is below 1,
/// preventing unnecessary loss of precision or intermediate underflow.
fn weighted_cross_product(left: f64, right: f64, weight: f64) -> f64 {
    let (larger, smaller) = if left.abs() >= right.abs() {
        (left, right)
    } else {
        (right, left)
    };
    if weight <= 1.0 {
        (larger * weight) * smaller
    } else {
        larger * (smaller * weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_dimensions_return_error() {
        assert!(RvCoefficient::new(0, 2).is_err());
        assert!(RvCoefficient::new(2, 0).is_err());
        assert!(RvCoefficient::new_equal(0).is_err());
        assert!(RvCoefficient::new(usize::MAX, 2).is_err());
        assert!(RvCoefficient::new(2, usize::MAX).is_err());
    }

    #[test]
    fn empty_and_single_observation_return_none() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        assert!(rv.is_empty());
        assert_eq!(rv.count(), 0);
        assert_eq!(rv.rv_coefficient(), None);
        assert_eq!(rv.adjusted_rv_coefficient(), None);
        assert_eq!(rv.mean_x(), None);
        assert_eq!(rv.mean_y(), None);
        assert_eq!(rv.covariance_xx(), None);
        assert_eq!(rv.covariance_xy(), None);

        rv.add(&[1.0, 2.0], &[3.0, 4.0]).unwrap();
        assert_eq!(rv.count(), 1);
        assert!(!rv.is_empty());
        assert_eq!(rv.rv_coefficient(), None);
        assert_eq!(rv.adjusted_rv_coefficient(), None);
        assert_eq!(rv.mean_x(), Some(&[1.0, 2.0][..]));
        assert_eq!(rv.mean_y(), Some(&[3.0, 4.0][..]));
    }

    #[test]
    fn univariate_case_matches_squared_pearson_correlation() {
        // In the 1D case (p=1, q=1), RV coefficient is exactly r^2.
        let mut rv = RvCoefficient::new(1, 1).unwrap();
        let x_vals = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y_vals = [2.0, 3.5, 4.5, 6.0, 8.0];

        for (&x, &y) in x_vals.iter().zip(&y_vals) {
            rv.add(&[x], &[y]).unwrap();
        }

        // Calculate standard Pearson r:
        // x mean = 3, y mean = 4.8
        // dx = [-2, -1, 0, 1, 2] -> sum dx^2 = 10
        // dy = [-2.8, -1.3, -0.3, 1.2, 3.2] -> sum dy^2 = 7.84 + 1.69 + 0.09 + 1.44 + 10.24 = 21.3
        // sum dx*dy = 5.6 + 1.3 + 0 + 1.2 + 6.4 = 14.5
        // r = 14.5 / sqrt(10 * 21.3) = 14.5 / sqrt(213) = 0.9935327...
        // r^2 = 14.5^2 / 213 = 210.25 / 213 = 0.9870892...
        let expected_r2 = (14.5 * 14.5) / (10.0 * 21.3);
        let computed_rv = rv.rv_coefficient().unwrap();

        assert!((computed_rv - expected_r2).abs() < 1e-12);
        // Adjusted RV is None for p=1, q=1 by definition
        assert_eq!(rv.adjusted_rv_coefficient(), None);
    }

    #[test]
    fn perfectly_correlated_multivariate_rotation_is_one() {
        let mut rv = RvCoefficient::new_equal(2).unwrap();
        // Y = 3 * Rotation(pi/2) * X
        let points = [
            ([1.0, 0.0], [0.0, 3.0]),
            ([0.0, 1.0], [-3.0, 0.0]),
            ([-1.0, 0.0], [0.0, -3.0]),
            ([0.0, -1.0], [3.0, 0.0]),
        ];

        for (x, y) in points {
            rv.add(&x, &y).unwrap();
        }

        let coeff = rv.rv_coefficient().unwrap();
        assert!((coeff - 1.0).abs() < 1e-12);
    }

    #[test]
    fn independent_uncorrelated_vectors_have_zero_rv() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        // Cartesian product of zero-mean vectors: zero cross-covariance
        let x_points = [[1.0, 0.0], [-1.0, 0.0], [0.0, 1.0], [0.0, -1.0]];
        let y_points = [[1.0, 1.0], [-1.0, 1.0], [1.0, -1.0], [-1.0, -1.0]];

        for x in &x_points {
            for y in &y_points {
                rv.add(x, y).unwrap();
            }
        }

        let coeff = rv.rv_coefficient().unwrap();
        assert!(coeff < 1e-12, "expected ~0, got {coeff}");

        let cov_xy = rv.covariance_xy().unwrap();
        for row in cov_xy {
            for val in row {
                assert!(
                    val.abs() < 1e-12,
                    "expected zero cross-covariance, got {val}"
                );
            }
        }
    }

    #[test]
    fn asymmetric_dimensions_work_properly() {
        // p = 3, q = 2
        let mut rv = RvCoefficient::new(3, 2).unwrap();
        assert_eq!(rv.p(), 3);
        assert_eq!(rv.q(), 2);

        rv.add(&[1.0, 2.0, 3.0], &[4.0, 5.0]).unwrap();
        rv.add(&[2.0, 3.0, 5.0], &[5.0, 7.0]).unwrap();
        rv.add(&[3.0, 5.0, 7.0], &[8.0, 10.0]).unwrap();
        rv.add(&[5.0, 8.0, 11.0], &[11.0, 14.0]).unwrap();

        let coeff = rv.rv_coefficient().unwrap();
        assert!((0.0..=1.0).contains(&coeff));
        assert!(
            coeff > 0.95,
            "strong linear trend should have high RV: {coeff}"
        );

        let cov_xy = rv.covariance_xy().unwrap();
        assert_eq!(cov_xy.len(), 3);
        assert_eq!(cov_xy[0].len(), 2);
    }

    #[test]
    fn zero_variance_returns_none() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        // X varies, but Y is constant
        rv.add(&[1.0, 2.0], &[5.0, 5.0]).unwrap();
        rv.add(&[2.0, 3.0], &[5.0, 5.0]).unwrap();
        rv.add(&[3.0, 4.0], &[5.0, 5.0]).unwrap();

        assert_eq!(rv.rv_coefficient(), None);
        assert_eq!(rv.adjusted_rv_coefficient(), None);
    }

    #[test]
    fn batch_merge_matches_direct_stream() {
        let data = [
            ([1.0, 2.0], [3.0, 4.0, 5.0]),
            ([2.0, 1.0], [4.0, 2.0, 6.0]),
            ([3.0, 5.0], [2.0, 7.0, 1.0]),
            ([4.0, 3.0], [5.0, 3.0, 8.0]),
            ([5.0, 6.0], [6.0, 8.0, 4.0]),
            ([6.0, 4.0], [7.0, 5.0, 9.0]),
        ];

        let mut direct = RvCoefficient::new(2, 3).unwrap();
        for (x, y) in data {
            direct.add(&x, &y).unwrap();
        }

        let mut batch1 = RvCoefficient::new(2, 3).unwrap();
        for (x, y) in &data[..3] {
            batch1.add(x, y).unwrap();
        }

        let mut batch2 = RvCoefficient::new(2, 3).unwrap();
        for (x, y) in &data[3..] {
            batch2.add(x, y).unwrap();
        }

        batch1.merge(&batch2).unwrap();

        assert_eq!(direct.count(), batch1.count());
        assert_eq!(direct.mean_x().unwrap(), batch1.mean_x().unwrap());
        assert_eq!(direct.mean_y().unwrap(), batch1.mean_y().unwrap());

        let direct_rv = direct.rv_coefficient().unwrap();
        let merged_rv = batch1.rv_coefficient().unwrap();
        assert!(
            (direct_rv - merged_rv).abs() < 1e-12,
            "direct={direct_rv}, merged={merged_rv}"
        );

        let direct_cov = direct.covariance_xy().unwrap();
        let merged_cov = batch1.covariance_xy().unwrap();
        for i in 0..2 {
            for j in 0..3 {
                assert!(
                    (direct_cov[i][j] - merged_cov[i][j]).abs() < 1e-12,
                    "at [{i}][{j}]: direct={}, merged={}",
                    direct_cov[i][j],
                    merged_cov[i][j]
                );
            }
        }
    }

    #[test]
    fn clear_resets_state_and_allows_reuse() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        rv.add(&[1.0, 2.0], &[3.0, 4.0]).unwrap();
        rv.add(&[2.0, 4.0], &[5.0, 7.0]).unwrap();
        assert!(rv.rv_coefficient().is_some());

        rv.clear();
        assert_eq!(rv.count(), 0);
        assert!(rv.is_empty());
        assert_eq!(rv.rv_coefficient(), None);

        // Re-adding works seamlessly
        rv.add(&[1.0, 0.0], &[2.0, 0.0]).unwrap();
        rv.add(&[0.0, 1.0], &[0.0, 2.0]).unwrap();
        assert_eq!(rv.count(), 2);
        assert!((rv.rv_coefficient().unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn rejected_inputs_preserve_state() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        rv.add(&[1.0, 2.0], &[3.0, 4.0]).unwrap();

        // Dimension mismatch
        assert!(rv.add(&[1.0], &[3.0, 4.0]).is_err());
        assert!(rv.add(&[1.0, 2.0], &[3.0]).is_err());
        assert_eq!(rv.count(), 1);

        // Non-finite coordinates
        assert!(rv.add(&[f64::NAN, 2.0], &[3.0, 4.0]).is_err());
        assert!(rv.add(&[1.0, 2.0], &[3.0, f64::INFINITY]).is_err());
        assert_eq!(rv.count(), 1);

        // Incompatible merge
        let other = RvCoefficient::new(3, 2).unwrap();
        assert!(rv.merge(&other).is_err());
        assert_eq!(rv.count(), 1);
    }

    #[test]
    fn adjusted_rv_coefficient_behavior() {
        let mut rv = RvCoefficient::new(2, 2).unwrap();
        // Collinear 2D vectors: adjusted RV is 1.0
        let data = [
            ([1.0, 2.0], [1.0, 2.0]),
            ([2.0, 4.0], [2.0, 4.0]),
            ([4.0, 1.0], [4.0, 1.0]),
            ([3.0, 5.0], [3.0, 5.0]),
        ];
        for (x, y) in data {
            rv.add(&x, &y).unwrap();
        }

        let adj = rv.adjusted_rv_coefficient().unwrap();
        assert!((adj - 1.0).abs() < 1e-12, "expected 1.0, got {adj}");

        // For p=1 or q=1, adjusted RV must return None
        let mut rv_1d = RvCoefficient::new(1, 2).unwrap();
        rv_1d.add(&[1.0], &[1.0, 2.0]).unwrap();
        rv_1d.add(&[2.0], &[2.0, 4.0]).unwrap();
        assert_eq!(rv_1d.adjusted_rv_coefficient(), None);
    }

    #[test]
    fn population_and_sample_covariance_denominators() {
        let mut rv = RvCoefficient::new(1, 1).unwrap();
        rv.add(&[2.0], &[4.0]).unwrap();
        rv.add(&[4.0], &[8.0]).unwrap();

        // Sample (n - 1 = 1)
        assert_eq!(rv.covariance_xx().unwrap()[0][0], 2.0);
        assert_eq!(rv.covariance_yy().unwrap()[0][0], 8.0);
        assert_eq!(rv.covariance_xy().unwrap()[0][0], 4.0);

        // Population (n = 2)
        assert_eq!(rv.population_covariance_xx().unwrap()[0][0], 1.0);
        assert_eq!(rv.population_covariance_yy().unwrap()[0][0], 4.0);
        assert_eq!(rv.population_covariance_xy().unwrap()[0][0], 2.0);
    }

    #[test]
    fn empty_merge_branches_preserve_state() {
        let mut populated = RvCoefficient::new(2, 2).unwrap();
        populated.add(&[1.0, 2.0], &[3.0, 4.0]).unwrap();
        populated.add(&[2.0, 3.0], &[4.0, 5.0]).unwrap();

        let empty = RvCoefficient::new(2, 2).unwrap();

        // Populated merged with empty leaves populated intact
        let mut target = populated.clone();
        target.merge(&empty).unwrap();
        assert_eq!(target.count(), 2);
        assert_eq!(target.rv_coefficient(), populated.rv_coefficient());

        // Empty merged with populated copies populated
        let mut empty_target = empty.clone();
        empty_target.merge(&populated).unwrap();
        assert_eq!(empty_target.count(), 2);
        assert_eq!(empty_target.rv_coefficient(), populated.rv_coefficient());
    }

    #[test]
    fn observation_overflow_preserves_state() {
        let mut rv = RvCoefficient::new(1, 1).unwrap();
        rv.count = u64::MAX;
        assert_eq!(
            rv.add(&[1.0], &[2.0]),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(rv.count, u64::MAX);

        let mut other = RvCoefficient::new(1, 1).unwrap();
        other.count = 1;
        assert_eq!(rv.merge(&other), Err(SketchError::ObservationCountOverflow));
        assert_eq!(rv.count, u64::MAX);
    }
}
