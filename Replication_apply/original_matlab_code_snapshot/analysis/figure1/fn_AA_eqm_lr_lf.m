% This function calculates equilibrium distribution of commuters and residents and world welfare, 
% given adjacency matrix, productivity, amenities,
% and parameters

function [chi_out, l_R_out, l_F_out, tau, Lij, Xi_ij] = fn_AA_eqm_lr_lf(t_bar, T_bar, u_bar, L_bar, theta, lambda, alpha, beta, tol, slack, maxiter)
N = length(T_bar);

lr_lf_to_x = [(1 - beta*theta) (theta*lambda*(1 - alpha*theta)/(1 + theta*lambda)); ...
                (theta*lambda*(1 - beta*theta)/(1 + theta*lambda))  (1 - alpha*theta)];
       
x_to_lr_lf = inv(lr_lf_to_x);

outer_tol = tol;
outer_iter = 0;
outer_diff = 1;
inner_tol = tol;
inner_update = 0.1;
lambda_sigfig_thresh = -log10(tol);

chi_lo = 0;
chi_hi = 100;
x1_0 = ones(N,1);
x2_0 = ones(N,1);

%% the loop
while outer_diff>outer_tol && outer_iter<maxiter
    outer_iter = outer_iter + 1;
    chi_0 = (chi_lo + chi_hi)/2;
    
    inner_diff = 1;
    inner_iter = 0;
    while inner_diff>inner_tol && inner_iter<maxiter 
        inner_iter = inner_iter + 1;
        [x1_0, x2_0] = assert_scale(x1_0, x2_0, x_to_lr_lf, lr_lf_to_x); 
        [l_R,l_F] = get_lr_lf(x1_0, x2_0, x_to_lr_lf);
        [x1_1, x2_1] = fn_AA_calc_eqm_commuting_static(t_bar, T_bar, u_bar, chi_0, l_R, l_F, L_bar, theta, lambda, alpha, beta);
        [x1_1, x2_1] = assert_scale(x1_1, x2_1, x_to_lr_lf, lr_lf_to_x); 
        
        if max(isnan(x1_1))== 1
            keyboard
        end
        
        inner_diff = norm(log(x1_1) - log(x1_0)) + norm(log(x2_1) - log(x2_0));
       
        
        x1_0 = x1_1.*inner_update + x1_0.*(1 - inner_update);
        x2_0 = x2_1.*inner_update + x2_0.*(1 - inner_update);
        
        if inner_iter == maxiter
            disp(strcat('Warning: maximum iterations reached, difference in last iteration:',num2str(inner_diff)))
            break
        end
    end
    
    if inner_iter == maxiter
        break
    end
    
    outer_diff = norm(log(chi_hi) - log(chi_lo));
    
 % Recovering scale 
    [scale_l_R, scale_l_F] = get_lr_lf(x1_0, x2_0, x_to_lr_lf);   
    [scale_x1, scale_x2] = fn_AA_calc_eqm_commuting_static(t_bar, T_bar, u_bar, chi_0, scale_l_R, scale_l_F, L_bar, theta, lambda, alpha, beta);
       lambda1 = x1_1 ./ scale_x1;
       lambda2 = x2_1 ./ scale_x2;

   if round(lambda1, lambda_sigfig_thresh)==1 & round(lambda2, lambda_sigfig_thresh)==1
       disp('Chi converged. Counterfactual equilibrium found.')
       break
   elseif lambda1>1 & lambda2>1
       chi_lo = chi_0;
   elseif lambda1<1 & lambda2<1
       chi_hi = chi_0;
   else
       error('Conflicting lambdas. Error.')
   end
end

%% one last check to see if the equation is solved
[final_eqm1, final_eqm2] = fn_AA_calc_eqm_commuting_static(t_bar, T_bar, u_bar, chi_0, l_R, l_F, L_bar, theta, lambda, alpha, beta);
[eqm1_check, eqm2_check] = get_lr_lf(l_R, l_F, lr_lf_to_x);

assert(sum(l_R) - 1 < outer_tol)
assert(sum(l_F) - 1 < outer_tol)
assert(isequal(round(lambda1, lambda_sigfig_thresh), round(lambda2, lambda_sigfig_thresh)))

if norm(log(final_eqm1) - log(eqm1_check)) > outer_tol*10^(slack)
    disp(strcat('Warning: Eqm. eq. 1 not solved. Distance between LHS and RHS is', num2str(norm(final_eqm1 - eqm1_check))))
end

if norm(log(final_eqm2) - log(eqm2_check)) > outer_tol*10^(slack)
    disp(strcat('Warning: Eqm. eq. 2 not solved. Distance between LHS and RHS is', num2str(norm(final_eqm2 - eqm2_check))))
end

l_R_out = l_R;
l_F_out = l_F;
chi_out = chi_0;

%% generating other outputs
% recovering the trade cost matrix
t = chi_0.^(-lambda/(1 + theta*lambda)) * L_bar.^(lambda/(1 + theta*lambda)) * t_bar.^(1/(1 + theta*lambda)) .* (u_bar.^(-theta*lambda/(1 + theta*lambda)))' .* ...
    T_bar.^(-theta*lambda/(1 + theta*lambda)) .* (l_R.^(lambda*(1 - beta*theta)/(1 + theta*lambda)))' .* l_F.^(lambda*(1 - alpha*theta)/(1 + theta*lambda));
tau = (inv(eye(N,N) - t.^-theta)).^(-1/theta);

% recovering the bilateral commuting matrix
Lij = chi_0 * tau.^(-theta) .* (T_bar.^theta)' .* u_bar.^theta .* l_R.^(beta*theta) .* (l_F.^(alpha*theta))';
    
% recover OD traffic matrix
Xi_ij = chi_0^(-1/(1 +theta*lambda)) * L_bar.^(1/(1 + theta*lambda)) * t_bar.^(-theta/(1 + theta*lambda)) .* T_bar.^(-theta/(1 + theta*lambda)) .* (u_bar.^(-theta/(1 + theta*lambda)))' .* ...
        (l_R.^((1 - beta*theta)/(1 + theta*lambda)))' .* l_F.^((1 - alpha*theta)/(1 + theta*lambda));
    

end

%% Local functions for the loop
% function which takes some x1 and x2 as input, and rescales to assert sum(l_F) =
% 1 and sum(l_R) = 1
function [x1_adj, x2_adj] = assert_scale(x1, x2, inv_mat, mat)
    [l_R, l_F] = get_lr_lf(x1, x2, inv_mat);
    l_R = l_R ./ sum(l_R);
    l_F = l_F ./ sum(l_F);
    
    [x1_adj, x2_adj] = get_lr_lf(l_R, l_F, mat);
end

 % function which calculates l_F and l_R given guess of x (or x's
 % given l_F and l_R)
 function [l_R, l_F] = get_lr_lf(x1, x2, mat)
        temp = exp(mat * [log(x1)'; log(x2)']);
        l_R = temp(1,:)'; 
        l_F = temp(2,:)';
 end
