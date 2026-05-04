%% Example of code for recovering commuting flows from observed traffic
% last edited by Albert Chen on 22 Nov 2021
clear
bilateral_data = '../../counterfactuals/seattle/sparse_adjmat_seattle.csv';
node_data = '../../counterfactuals/seattle/node_lr_lf_seattle.csv';
commute_data = '../../counterfactuals/seattle/sparse_commute_seattle.csv';

%% Parameters
theta = 4;
delta0 = 1/theta; % 1/commuting elasticity
delta1 = .488; % derived from regressions
lambda = delta0*delta1;
alpha = 0.1;
beta = -0.3;

%% import traffic and hats as sparse matrices
bilateral = csvread(bilateral_data);
nodes = csvread(node_data);
commute = csvread(commute_data);
l_R = nodes(:,2);
l_F = nodes(:,3);
N = length(l_R);
sparse_traffic = sparse(bilateral(:,1), bilateral(:,2), bilateral(:,3), N, N);
sparse_commuters = sparse(commute(:,1), commute(:,2), commute(:,3), N, N);
sparse_t_bar_hat_asym = sparse(bilateral(:,1), bilateral(:,2), .99, N, N);
Xi_ij = full(sparse_traffic);
Lij = full(sparse_commuters);
%% let's try making population consistent
l_R = mean([sum(l_R) sum(l_F)]) * (l_R./sum(l_R));
l_F = mean([sum(l_R) sum(l_F)]) * (l_F./sum(l_F));


%%
avg_pop = (l_R + l_F)/2;
avg_flow = (sum(Xi_ij,2) + sum(Xi_ij',2))/2;
B = inv(avg_pop.*eye(N,N) + avg_flow.*eye(N,N) - Xi_ij);
Lij_pred = B .* l_R .* l_F';

%%
lij = Lij./sum(Lij,'all');
lij_pred = Lij_pred./sum(Lij_pred, 'all');

[h_index, w_index, val] = find(Lij_pred);
csvwrite('../../data/seattle/derived/predicted_lij.csv', [h_index w_index val]);
