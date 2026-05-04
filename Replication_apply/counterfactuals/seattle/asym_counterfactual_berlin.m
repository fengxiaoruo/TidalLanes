%% Program for calculating effects of reducing iceberg trade costs by 1% along a direction of a edge of the Seattle Road Network
clear
delete(gcp('nocreate'))
format long
bilateral_data = 'sparse_adjmat_seattle.csv';
node_data = 'node_lr_lf_seattle.csv';

%% Berlin Wall Parameters
theta = 6.83;
delta0 = 1/theta; % 1/commuting elasticity
delta1 = .488; % derived from regressions
lambda = delta0*delta1;
alpha = -0.12;
beta = -0.1;

%% import traffic and hats as sparse matrices
bilateral = readmatrix(bilateral_data);
nodes = readmatrix(node_data);
l_R = nodes(:,2);
l_F = nodes(:,3);
N = length(l_R);
sparse_traffic = sparse(bilateral(:,1), bilateral(:,2), bilateral(:,3), N, N);
sparse_t_bar_hat_asym = sparse(bilateral(:,1), bilateral(:,2), .99, N, N);

%% get adjacency matrix, figure out mapping
t_bar_hat_asym = full(sparse_t_bar_hat_asym); t_bar_hat_asym(eye(N,N) == 1) = 0; 
Xi_ij = full(sparse_traffic);
T_bar_hat = ones(N,1);
u_bar_hat = ones(N,1);
L_bar_hat = 1;
L_bar = mean([sum(l_R) sum(l_F)]);
l_R = l_R./sum(l_R);
l_F = l_F./sum(l_F);

[row, col, val] = find(t_bar_hat_asym);
loc = [row col val];

%% Run the program
parpool(30);
m = length(loc);
parfor i = 1:m
    t_bar_hat_iter = ones(N,N);
    index = loc(i,:);
    t_bar_hat_iter(index(1), index(2)) = index(3);
    [chi_lr_lf_hat, t_hat, Xi_ij_hat, err_eqm1, err_eqm2] = fn_AA_eqm_lr_lf_counterfactual(t_bar_hat_iter, T_bar_hat, u_bar_hat, L_bar_hat, l_R, l_F, L_bar, Xi_ij, theta, lambda, alpha, beta, 1e-8, 1, 200000);
    chi_lr_lf_export_name = strcat('chi_lr_lf_ber_', num2str(index(1)), '_', num2str(index(2)), '.csv');
    traffic_export_name = strcat('traffic_ber_', num2str(index(1)), '_', num2str(index(2)), '.csv');
    t_hat_export_name = strcat('t_hat_ber_', num2str(index(1)), '_', num2str(index(2)), '.csv');
    err_export_name = strcat('err_ber_', num2str(index(1)), '_', num2str(index(2)), '.csv');
    writematrix([index(1)*ones(N,1) index(2)*ones(N,1) (1:N)' chi_lr_lf_hat], chi_lr_lf_export_name);
    writematrix([index(1) index(2) err_eqm1 err_eqm2], err_export_name);
    writematrix(Xi_ij_hat, traffic_export_name);
    writematrix(t_hat, t_hat_export_name);
end
