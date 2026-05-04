% This function calculates the RHS of the static equilibrium equations for
% commuting given infrastructure, productivities, amenities, income, labor, and
% parameters

function [eqm1, eqm2] = fn_AA_calc_eqm_commuting_static(t_bar, T_bar, u_bar, chi, l_R, l_F, L_bar, theta, lambda, alpha, beta)
    eqm1 = calc_t1(T_bar, u_bar, chi, l_F, theta, lambda, alpha) + calc_sum1(t_bar, T_bar, u_bar, chi, l_R, L_bar, theta, lambda, beta);
    eqm2 = calc_t2(T_bar, u_bar, chi, l_R, theta, lambda, beta) + calc_sum2(t_bar, T_bar, u_bar, chi, l_F, L_bar, theta, lambda, alpha);
end

% function which calculates the first term in eqm. eq. 1
function t1 = calc_t1(T_bar, u_bar, chi, l_F, theta, lambda, alpha)
    t1 = chi * T_bar.^theta .* u_bar.^(theta) .* l_F.^(theta*alpha + (theta*lambda*(1 - alpha*theta)/(1 + theta*lambda)));
end

% function which calculates the first term in eqm. eq. 2
function t2 = calc_t2(T_bar, u_bar, chi, l_R, theta, lambda, beta)
    t2 = chi * T_bar.^theta .* u_bar.^(theta) .* l_R.^(theta*beta + (theta*lambda*(1 - beta*theta)/(1 + theta*lambda)));
end

% function which calculates the sum in eqm. eq. 1
function s1 = calc_sum1(t_bar, T_bar, u_bar, chi, l_R, L_bar, theta, lambda, beta)
    s1 = chi^(theta*lambda/(1 + theta*lambda)) * L_bar^(-theta*lambda/(1 + theta*lambda)) * sum(t_bar.^(-theta/(1 + theta*lambda)) .* T_bar.^((theta^2*lambda)/(1 + theta*lambda)) .* ...
            u_bar.^(theta) .* (u_bar.^(-theta/(1 + theta*lambda)))' .* (l_R.^((1 - beta*theta)/(1 + theta*lambda)))', 2);
end

% function which calculates the sum in eqm. eq. 2
function s2 = calc_sum2(t_bar, T_bar, u_bar, chi, l_F, L_bar, theta, lambda, alpha)
    s2 = chi^(theta*lambda/(1 + theta*lambda)) * L_bar^(-theta*lambda/(1 + theta*lambda)) * sum((t_bar.^(-theta/(1 + theta*lambda)))' .* T_bar.^(theta) .* u_bar.^((theta^2*lambda)/(1 + theta*lambda)) .* ...
           (T_bar.^(-theta/(1 + theta*lambda)))' .* (l_F.^((1 - alpha*theta)/(1 + theta*lambda)))', 2);
end