%% Generating link intensity for Seattle
% last revised 2021 Nov 22 by Albert Chen
clear
bilateral_data = '../../counterfactuals/seattle/sparse_adjmat_seattle.csv';
node_data = '../../counterfactuals/seattle/node_lr_lf_seattle.csv';
commute_data = '../../counterfactuals/seattle/sparse_commute_seattle.csv';

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


%% Calculating and inverting matrix
avg_pop = (l_R + l_F)/2;
avg_flow = (sum(Xi_ij,2) + sum(Xi_ij',2))/2;
A = sqrt((Xi_ij .* Xi_ij')./((avg_pop + avg_flow) .* (avg_pop' + avg_flow')));
B = inv(eye(N) - A);

%% between SafeCo Field (h_index = 84) and the University of Washington (w_index = 145)
pi = (B(84,:).* A .* B(:,145))/B(84,145);
[h_index, w_index, val] = find(pi);
link_intensity = array2table([h_index w_index val], 'VariableNames', {'h_index', 'w_index', 'pi'});


%% combining with index_id_xwalk
index_id_xwalk = readtable("../../data/seattle/derived/index_id_xwalk.csv", 'Format', '%s%s%u%u');
link_intensity_id = innerjoin(link_intensity, index_id_xwalk, 'LeftKeys', [1,2], 'RightKeys', [3,4]); 
link_intensity_id = addvars(link_intensity_id, strcat(link_intensity_id.h_id, link_intensity_id.w_id), 'NewVariableNames', {'joinid'});
writetable(link_intensity_id, '../../data/seattle/derived/seattle_link_intensity.csv');
