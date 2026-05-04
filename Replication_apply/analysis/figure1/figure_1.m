% This m-file presents a simple example of how the scale matters for this
clear
clc

%% Making the geography

    % parameters
        alpha = 0;
        beta = 0;
        theta = 4; % elasticity of commuting
        N = 25; % number of locations;
        
        
for L_bar = [100,1000,10000]
for lambda = [0,0.05]
        
        
    % location fundamentals
        
        T_bar = ones(N,1); % productivity of locations
        u_bar = ones(N,1); % amenities of locations
        
    % geography of locations
        
        % simulated iceberg costs, productivity, and amenity matrices
    % make a grid (coordinates)
            [X,Y] = meshgrid(1:N^0.5,1:N^0.5);

        % connecting locations to each other
            direct_cost = 1.5; % exogenous iceberg trade cost 
            adjacency_matrix = NaN(N,N);
            for i=1:N
                for j=1:N
                    adjacency_matrix(i,j) = double(abs(X(i)-X(j)) + abs(Y(i)-Y(j))==1);
                end
            end
            t_bar = direct_cost*adjacency_matrix;  % exogenous network component
            t_bar(t_bar == 0) = Inf;

            
%% Calculating the initial equilibrium

    tol = 1e-6;
    slack = 0;
    maxiter = 10000;
    [chi, l_R, l_F, tau, Lij, Xi_ij] = fn_AA_eqm_lr_lf(t_bar, T_bar, u_bar, L_bar, theta, lambda, alpha, beta, tol, slack, maxiter);
    
%% Making a figure

    Xi_ij_sym = 0.5*(Xi_ij + Xi_ij');
    
    scale_factor = 100;
    
    % getting the right scale of traffic
        G = graph(Xi_ij_sym);
        EColor = (G.Edges.Weight);

    G = graph(Xi_ij_sym);
    f = figure(1)
        clf
%         naam = strcat('\lambda',' = ',num2str(lambda),', population = ',num2str(L_bar));
%         title(naam,'Interpreter','tex')
%         f.PaperSize = [18,13.5]
        f.PaperSize = [12,12]
        ax1 = axes;
        p = plot(G,'XData',X(:),'YData',Y(:),'LineWidth',12*ones(size(EColor)),'EdgeCData',EColor)
        set(gca,'Visible','off')
%         caxis([0*(L_bar/1000),10*(L_bar/1000)])
        view(ax1,[-45,85])
        ax2 = axes;
        hold on
        for i=1:N
            plot3([X(i);X(i)],[Y(i);Y(i)],[0;scale_factor*(l_R(i)-0.025)],'LineWidth',20,'Color',[0.7 0.7 0.7])
        end
        scatter3(X(:),Y(:),scale_factor*(l_R-0.025),500*ones(N,1),l_R,'filled','d','MarkerEdgeColor',[0.7 0.7 0.7])
        hold off
        caxis([0.025,0.054])
        view(ax2,[-45,85])
        linkaxes([ax1,ax2])
        linkprop([ax1,ax2],{'CameraPosition','CameraUpVector'})
%         Link = linkprop([ax1, ax2],{'XLim', 'YLim', 'ZLim'});
        %%Hide the top axes
        ax2.Visible = 'off';
        ax2.XTick = [];
        ax2.YTick = [];
        %%Give each one its own colormap
        colormap(ax1,'cool')
        colormap(ax2,'jet')
        %%Then add colorbars and get everything lined up
        set([ax1,ax2],'Position',[-0.06 -0.05 1.1 1.1]);
        cb1 = colorbar(ax1,'Position',[0.03 .3 .03 .5]);
        cb2 = colorbar(ax2,'Position',[0.93 .3 .03 .5]);
        set(ax1, 'XTick', [], 'XTickLabel', '');
        set(ax1, 'YTick', [], 'YTickLabel', '');
        set(ax1, 'ZTick', [], 'ZTickLabel', '');
        set(ax2, 'XTick', [], 'XTickLabel', '');
        set(ax2, 'YTick', [], 'YTickLabel', '');
        set(ax2, 'ZTick', [], 'ZTickLabel', '');
        
        filename = strcat('../../outputs/', 'grid_lambda',num2str(lambda),'_Lbar',num2str(L_bar),'.pdf')
        print(filename,'-dpdf','-bestfit')

end
end
