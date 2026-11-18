# Congestion Elasticity Parameter Mapping

This note records how the congestion elasticity in the TidalLanes model maps to the empirical estimates in Yang, Purevjav, and Li (2020), Barwick et al. (2024), and the Allen-Arkolakis (AA) transportation-cost convention.

## 1. Objects and Notation

Let

$$
S_{kl} = \text{speed on link } (k,l),
$$

$$
D_{kl} = \text{traffic density on link } (k,l),
$$

where the Beijing papers define density as

# $$
D_{kl}

# \frac{\text{traffic volume}*{kl}}{\text{speed}*{kl} \times \text{lanes}_{kl}}

\frac{\text{vehicles}}{\text{lane-km}}.
$$

Equivalently, if link traffic is measured as a stock of vehicles or trips assigned to link $(k,l)$, the model-consistent density object should be

# $$
D_{kl}

\frac{\xi_{kl}}{n_{kl}\ell_{kl}},
$$

where $\xi_{kl}$ is directed link traffic, $n_{kl}$ is the directed lane count, and $\ell_{kl}$ is link length.

The inverse speed, or travel time per unit distance, is

$$
S_{kl}^{-1}.
$$

If physical travel time on a link is

$$
\tau_{kl}=\frac{\ell_{kl}}{S_{kl}},
$$

then, holding link length fixed, the elasticity of physical travel time with respect to density equals the elasticity of inverse speed with respect to density.

## 2. Yang et al. (AEJ Policy 2020)

Yang, Purevjav, and Li estimate a level speed-density equation:

# $$
S_{it}

\alpha
+
\beta D_{it}
+
X_{it}'\gamma
+
\text{fixed effects}
+
\varepsilon_{it}.
$$

Their preferred IV estimate is approximately

$$
\hat{\beta}=-1.098.
$$

This is not a log-log elasticity. Its interpretation is:

$$
\frac{\partial S}{\partial D}
\approx
-1.098,
$$

so one additional vehicle per lane-km lowers speed by about 1.1 km/hour.

To convert this level slope into a speed-density elasticity at a given density-speed point $(D,S)$:

# $$
\varepsilon_{S,D}

# \frac{\partial S}{\partial D}\frac{D}{S}

\beta\frac{D}{S}.
$$

The corresponding inverse-speed elasticity is

# $$
\varepsilon_{S^{-1},D}

# -\varepsilon_{S,D}

-\beta\frac{D}{S}.
$$

Therefore, Yang et al. provide a strong empirical benchmark for the speed-density relationship, but their coefficient should not be inserted directly as the TidalLanes congestion elasticity unless it is first converted to a log elasticity at a chosen baseline density and speed.

## 3. Barwick et al. (AER 2024)

Barwick et al. use the same Beijing traffic sensor data as Yang et al. to estimate a log-log speed-density relationship:

# $$
\ln S_{st}

e_r \ln D_{st}
+
X_{st}'\beta
+
\varepsilon_{st}.
$$

Their city-wide IV estimate is approximately

$$
\hat{e}\approx -1.1.
$$

This is directly a speed-density elasticity:

# $$
\frac{\partial \ln S}{\partial \ln D}

\hat{e}
\approx
-1.1.
$$

Since inverse speed is $S^{-1}$,

# $$
\ln S^{-1}

-\ln S,
$$

so the inverse-speed elasticity is

# $$
\frac{\partial \ln S^{-1}}{\partial \ln D}

# -\frac{\partial \ln S}{\partial \ln D}

-\hat{e}
\approx
1.1.
$$

Thus Barwick et al.'s estimate can be mapped into a congestion elasticity for inverse speed or physical travel time as

# $$
\delta_1

-\hat{e}
\approx
1.1.
$$

## 4. AA Transportation-Cost Convention

AA define transportation cost as a log-linear function of physical travel time:

# $$
t_{kl}

\left(\text{distance}*{kl}\times \text{speed}*{kl}^{-1}\right)^{\delta_0}.
$$

They set

$$
\delta_0=\frac{1}{\theta},
$$

where $\theta$ is the Frechet shape parameter.

They then assume inverse speed is log-linear in traffic congestion:

# $$
\text{speed}_{kl}^{-1}

m_0
\left(
\frac{\Xi_{kl}}{\text{lanes}*{kl}}
\right)^{\delta_1}
\varepsilon*{kl}.
$$

Substituting the inverse-speed equation into the transportation-cost equation gives

# $$
t_{kl}

\bar{t}*{kl}
\left(
\frac{\Xi*{kl}}{\text{lanes}*{kl}}
\right)^{\lambda*{\mathrm{AA}}},
$$

where

# $$
\lambda_{\mathrm{AA}}

# \delta_0\delta_1

\frac{\delta_1}{\theta}.
$$

Thus AA's estimated $\delta_1$ is the inverse-speed congestion elasticity, while the parameter that enters the iceberg transportation-cost equation is smaller by a factor of $\theta$.

## 5. TidalLanes Physical-Time Convention

The physical-time version of the TidalLanes congestion technology is

# $$
\tau_{kl}

\bar{\tau}*{kl}
D*{kl}^{\lambda_{\mathrm{phys}}},
$$

or, equivalently,

# $$
\ln \tau_{kl}

\ln \bar{\tau}*{kl}
+
\lambda*{\mathrm{phys}}\ln D_{kl}.
$$

Since

$$
\tau_{kl}=\frac{\ell_{kl}}{S_{kl}},
$$

holding link length fixed implies

# $$
\ln \tau_{kl}

## \ln \ell_{kl}

\ln S_{kl}.
$$

Therefore,

# $$
\lambda_{\mathrm{phys}}

-\frac{\partial \ln S_{kl}}{\partial \ln D_{kl}}.
$$

Using Barwick et al.'s Beijing estimate,

# $$
\lambda_{\mathrm{phys}}

-\hat{e}
\approx
1.1.
$$

## 6. TidalLanes AA/Iceberg Convention

If the TidalLanes implementation follows the AA iceberg-cost convention,

# $$
t_{kl}

\tau_{kl}^{1/\theta},
$$

and physical travel time satisfies

# $$
\tau_{kl}

\bar{\tau}*{kl}
D*{kl}^{\lambda_{\mathrm{phys}}},
$$

then

# $$
t_{kl}

# \left(\bar{\tau}*{kl}D*{kl}^{\lambda_{\mathrm{phys}}}\right)^{1/\theta}

\bar{t}*{kl}D*{kl}^{\lambda_{\mathrm{phys}}/\theta}.
$$

Hence the iceberg-cost congestion elasticity is

# $$
\lambda_{\mathrm{AA}}

\frac{\lambda_{\mathrm{phys}}}{\theta}.
$$

Using Barwick et al.'s estimate,

# $$
\lambda_{\mathrm{AA}}

\frac{1.1}{\theta}.
$$

With the current TidalLanes calibration

$$
\theta=6.83,
$$

this gives

# $$
\lambda_{\mathrm{AA}}

\frac{1.1}{6.83}
\approx
0.161.
$$

## 7. Mapping Table


| Source or model                  | Empirical/model equation                                  | Estimated or calibrated object | Mapping to physical-time elasticity           | Mapping to AA iceberg-cost elasticity                |
| -------------------------------- | --------------------------------------------------------- | ------------------------------ | --------------------------------------------- | ---------------------------------------------------- |
| Yang et al. (2020)               | $S=\alpha+\beta D+\cdots$                                 | $\hat{\beta}\approx -1.098$    | $\lambda_{\mathrm{phys}}=-\hat{\beta}D/S$     | $\lambda_{\mathrm{AA}}=(-\hat{\beta}D/S)/\theta$     |
| Barwick et al. (2024)            | $\ln S=e\ln D+\cdots$                                     | $\hat{e}\approx -1.1$          | $\lambda_{\mathrm{phys}}=-\hat{e}\approx 1.1$ | $\lambda_{\mathrm{AA}}=-\hat{e}/\theta\approx 0.161$ |
| AA calibration currently in code | $S^{-1}=m_0(\Xi/\text{lanes})^{\delta_1}\varepsilon$      | $\delta_1=0.488$               | $\lambda_{\mathrm{phys}}=0.488$               | $\lambda_{\mathrm{AA}}=0.488/\theta\approx 0.071$    |
| TidalLanes physical-time version | $\tau=\bar{\tau}D^{\lambda_{\mathrm{phys}}}$              | $\lambda_{\mathrm{phys}}$      | direct                                        | $\lambda_{\mathrm{phys}}/\theta$                     |
| TidalLanes AA/iceberg version    | $t=\bar{t}D^{\lambda_{\mathrm{AA}}}$, $t=\tau^{1/\theta}$ | $\lambda_{\mathrm{AA}}$        | $\theta\lambda_{\mathrm{AA}}$                 | direct                                               |


## 8. Recommendation for TidalLanes

The Beijing estimate in Barwick et al. is the closest external parameter for the TidalLanes congestion technology because it is a log-log elasticity of speed with respect to traffic density measured in vehicles per lane-km.

If the model is written in physical travel time, use

$$
\lambda_{\mathrm{phys}}
\approx
1.1.
$$

If the implemented model follows the AA iceberg-cost convention with

$$
t_{kl}=\tau_{kl}^{1/\theta},
$$

then use

# $$
\lambda_{\mathrm{AA}}

\frac{1.1}{\theta}
\approx
0.161
\quad \text{when } \theta=6.83.
$$

The current code uses

# $$
\lambda_{\mathrm{code}}

\frac{0.488}{6.83}
\approx
0.071,
$$

which is internally consistent with the AA Seattle calibration but is not the Beijing-specific Barwick calibration.

Before replacing the current placeholder, the congestion regressor in the model should be aligned with the Beijing papers' density definition:

# $$
D_{kl}

\frac{\xi_{kl}}{n_{kl}\ell_{kl}}.
$$

If the implementation instead uses $\xi_{kl}/n_{kl}$ without dividing by link length, then the parameter no longer has exactly the same interpretation as Barwick et al.'s vehicles-per-lane-km elasticity.