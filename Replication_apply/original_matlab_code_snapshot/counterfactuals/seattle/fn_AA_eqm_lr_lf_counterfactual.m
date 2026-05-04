% This function calculates equilibrium distribution of commuters and residents and world welfare, 
% given adjacency matrix, productivity, amenities,
% and parameters

function [chi_lR_lF_out, t_hat, Xi_ij_hat, err_eqm1, err_eqm2] = fn_AA_eqm_lr_lf_counterfactual(t_bar_hat, T_bar_hat, u_bar_hat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta, tol, slack, maxiter)
N = length(T_bar_hat);

lr_lf_to_x = [(1 - beta*theta) (theta*lambda*(1 - alpha*theta)/(1 + theta*lambda)); ...
                (theta*lambda*(1 - beta*theta)/(1 + theta*lambda))  (1 - alpha*theta)];
       
x_to_lr_lf = inv(lr_lf_to_x);

outer_tol = tol;
outer_update = .1;
outer_iter = 0;
outer_diff = 1;
inner_tol = tol;
inner_update = 0.1;
lambda_sigfig_thresh = -log10(tol);

[x1, x2] = sep_output(get_lr_lf(l_R, l_F, lr_lf_to_x));

%chihat_lo = 0;
%chihat_hi = 2;
chihat_0 = 1;
x1hat_0 = ones(N,1);
x2hat_0 = ones(N,1);

%% the loop
while outer_diff>outer_tol && outer_iter<maxiter
    outer_iter = outer_iter + 1;
    %chihat_0 = (chihat_lo + chihat_hi)/2;
    
    inner_diff = 1;
    inner_iter = 0;
    while inner_diff>inner_tol && inner_iter<maxiter 
        inner_iter = inner_iter + 1;
        
        % calculate counterfactuals
        [l_Rhat,l_Fhat] = sep_output(get_lr_lf(x1hat_0, x2hat_0, x_to_lr_lf));
        [x1hat_1, x2hat_1] = sep_output(fn_AA_calc_eqm_commuting_counterfactual(t_bar_hat, T_bar_hat, u_bar_hat, chihat_0, l_Rhat, l_Fhat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta));
            [x1hat_1, x2hat_1] = sep_output(assert_cf_scale(x1hat_1, x2hat_1, x1, x2, x_to_lr_lf, lr_lf_to_x)); 
        
        if max(isnan(x1hat_1))== 1
            keyboard
        end
        
        inner_diff = norm(log(x1hat_1) - log(x1hat_0)) + norm(log(x2hat_1) - log(x2hat_0));
       
        
        x1hat_0 = x1hat_1.*inner_update + x1hat_0.*(1 - inner_update);
        x2hat_0 = x2hat_1.*inner_update + x2hat_0.*(1 - inner_update);
        
        if inner_iter == maxiter
            disp(strcat('Warning: maximum iterations reached, difference in last iteration:',num2str(inner_diff)))
            break
        end
    end
    
    if inner_iter == maxiter
        break
    end
    
    
    
    % find by fmincon
    lambda_norm = @(chihat) norm(log(get_lr_lf(l_Rhat, l_Fhat, lr_lf_to_x)) - ...
                    log(fn_AA_calc_eqm_commuting_counterfactual(t_bar_hat, T_bar_hat, u_bar_hat, chihat, l_Rhat, l_Fhat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta)));
    options = optimoptions('fmincon', 'Display', 'notify-detailed', 'StepTolerance', outer_tol);             
    chihat_1 = fmincon(lambda_norm, 1, -1, 0, [], [], [], [], [], options);
    outer_diff = norm(log(chihat_1) - log(chihat_0));
    chihat_0 = outer_update*chihat_1 + (1 - outer_update)*chihat_0;
         
                
   
    
 % Recovering scale 
    [scale_l_Rhat, scale_l_Fhat] = sep_output(get_lr_lf(x1hat_0, x2hat_0, x_to_lr_lf));   
    [scale_xhat1, scale_xhat2] = sep_output(fn_AA_calc_eqm_commuting_counterfactual(t_bar_hat, T_bar_hat, u_bar_hat, chihat_0, scale_l_Rhat, scale_l_Fhat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta));
       lambda1 = scale_xhat1./x1hat_1;
       lambda2 = scale_xhat2./x2hat_1;
       
    if norm(log(lambda1)) < outer_tol*10^slack && norm(log(lambda2)) < outer_tol*10^slack
       disp('Scale is one. Counterfactual equilibrium found.')
       break
    end
    
     %{
    % Find by estimating chis
    chihats = [chihat_0/mean(lambda1) chihat_0/((mean(lambda1))^((1 + theta*lambda)/theta*lambda)); ...
                chihat_0/mean(lambda2) chihat_0/((mean(lambda2))^((1 + theta*lambda)/theta*lambda))];

    chihat_1 = mean(chihats, 'all');
    outer_diff = norm(log(chihat_1) - log(chihat_0));
    chihat_0 = outer_update*chihat_1 + (1 - outer_update)*chihat_0;
     %}
    
  %{
    % Find by bisection
   if lambda1 > 1 & lambda2 > 1
       chihat_lo = chihat_0;
   elseif lambda1 < 1 & lambda2 < 1
       chihat_hi = chihat_0;
   elseif round(lambda1, lambda_sigfig_thresh)==1 & round(lambda2, lambda_sigfig_thresh)==1
       disp('Chi converged. Counterfactual equilibrium found.')
       break
   else
       error("Conflicting lambdas")
   end
   %}
    
  
end

%% one last check to see if the equation is solved
[final_eqm1, final_eqm2] = sep_output(fn_AA_calc_eqm_commuting_counterfactual(t_bar_hat, T_bar_hat, u_bar_hat, chihat_0, l_Rhat, l_Fhat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta));
[eqm1_check, eqm2_check] = sep_output(get_lr_lf(l_Rhat, l_Fhat, lr_lf_to_x));

assert(sum(l_R) - 1 < outer_tol)
assert(sum(l_F) - 1 < outer_tol)

if norm(log(final_eqm1) - log(eqm1_check)) > outer_tol*10^(slack)
    disp(strcat('Warning: Eqm. eq. 1 not solved. Distance between LHS and RHS is', num2str(norm(final_eqm1 - eqm1_check))))
end

if norm(log(final_eqm2) - log(eqm2_check)) > outer_tol*10^(slack)
    disp(strcat('Warning: Eqm. eq. 2 not solved. Distance between LHS and RHS is', num2str(norm(final_eqm2 - eqm2_check))))
end

chi_lR_lF_out = [chihat_0*ones(N,1) l_Rhat l_Fhat];
err_eqm1 = norm(log(final_eqm1) - log(eqm1_check));
err_eqm2 = norm(log(final_eqm2) - log(eqm2_check));

%% generating other outputs
% recovering the trade cost matrix
t_hat = chihat_0.^(-lambda/(1 + theta*lambda))  * L_bar_hat.^(lambda/(1 + theta*lambda)) *  t_bar_hat.^(1/(1 + theta*lambda)) .* (u_bar_hat.^(-theta*lambda/(1 + theta*lambda)))' .* ...
    T_bar_hat.^(-theta*lambda/(1 + theta*lambda)) .* (l_Rhat.^(lambda*(1 - beta*theta)/(1 + theta*lambda)))' .* l_Fhat.^(lambda*(1 - alpha*theta)/(1 + theta*lambda));
    
% recover OD traffic matrix
Xi_ij_hat = chihat_0^(-1/(1 +theta*lambda)) * L_bar_hat.^(1/(1 + theta*lambda)) * t_bar_hat.^(-theta/(1 + theta*lambda)) .* T_bar_hat.^(-theta/(1 + theta*lambda)) .* (u_bar_hat.^(-theta/(1 + theta*lambda)))' .* ...
        (l_Rhat.^((1 - beta*theta)/(1 + theta*lambda)))' .* l_Fhat.^((1 - alpha*theta)/(1 + theta*lambda));
    

end

%% Functions for the loop
 % function which asserts that post- equilibrium state follows the adding
 % up constraint
 function xhat_adj = assert_cf_scale(x1hat, x2hat, x1, x2, inv_mat, mat)
    x1_prime = x1hat .* x1;
    x2_prime = x2hat .* x2;
    xhat_adj = assert_scale(x1_prime, x2_prime, inv_mat, mat) ./ [x1 x2];   
 end
 
% function which takes some x1 and x2 as input, and rescales to assert y =
% 1 and l = 1
function x_adj = assert_scale(x1, x2, inv_mat, mat)
    [l_R, l_F] = sep_output(get_lr_lf(x1, x2, inv_mat));
    l_R = l_R ./ sum(l_R);
    l_F = l_F ./ sum(l_F);
    x_adj = get_lr_lf(l_R, l_F, mat);
end


 % function which calculates income and labor (hat) given guess of x(hat)
 % or the reverse
 function output = get_lr_lf(x1, x2, mat)
        output = exp(mat * [log(x1)'; log(x2)'])';
 end
 
 % separating output of any function
 function [v1, v2] = sep_output(mat)
    v1 = mat(:,1);
    v2 = mat(:,2);
end