# FILE: modules/training/models.py

import torch
import torch.nn.functional as F
from torch_geometric.nn import GATConv, HeteroConv, Linear

class GNNEncoder(torch.nn.Module):
    def __init__(self, hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim,
                 heads=4, dropout=0.2, dropout_lin=0.4): # [변경] Config 파라미터 추가
        super().__init__()

        self.dropout_ratio = dropout
        self.dropout_lin_ratio = dropout_lin
        
        # 1. Input Projection
        self.news_lin = Linear(news_feat_dim, hidden_dim)
        self.keyword_lin = Linear(keyword_feat_dim, hidden_dim)
        self.stock_lin = Linear(stock_feat_dim, hidden_dim)

        # ------------------------------------------------------------------
        # Layer 1
        # ------------------------------------------------------------------
        self.conv1 = HeteroConv({
            ('news', 'has_keyword', 'keyword'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('keyword', 'rev_has_keyword', 'news'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('news', 'has_stock', 'stock'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('stock', 'rev_has_stock', 'news'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            
            # Global Edges
            ('keyword', 'globally_related', 'stock'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('stock', 'rev_globally_related', 'keyword'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
        }, aggr='sum')
        self.bn1 = torch.nn.BatchNorm1d(hidden_dim)

        # ------------------------------------------------------------------
        # Layer 2
        # ------------------------------------------------------------------
        self.conv2 = HeteroConv({
            ('news', 'has_keyword', 'keyword'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('keyword', 'rev_has_keyword', 'news'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('news', 'has_stock', 'stock'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
            ('stock', 'rev_has_stock', 'news'): GATConv((-1, -1), hidden_dim, heads=heads, concat=False, dropout=dropout, add_self_loops=False),
        }, aggr='sum')
        self.bn2 = torch.nn.BatchNorm1d(hidden_dim)

        # ------------------------------------------------------------------
        # Layer 3 (Output)
        # heads는 여기서 보통 1로 고정하지만, 필요하면 이것도 파라미터화 가능
        # ------------------------------------------------------------------
        self.conv3 = HeteroConv({
            ('news', 'has_keyword', 'keyword'): GATConv((-1, -1), out_dim, heads=1, concat=False, dropout=dropout, add_self_loops=False),
            ('keyword', 'rev_has_keyword', 'news'): GATConv((-1, -1), out_dim, heads=1, concat=False, dropout=dropout, add_self_loops=False),
            ('news', 'has_stock', 'stock'): GATConv((-1, -1), out_dim, heads=1, concat=False, dropout=dropout, add_self_loops=False),
            ('stock', 'rev_has_stock', 'news'): GATConv((-1, -1), out_dim, heads=1, concat=False, dropout=dropout, add_self_loops=False),
        }, aggr='sum')

    def forward(self, x_dict, edge_index_dict):
        x_dict = {
            'news': self.news_lin(x_dict['news']).relu(),
            'keyword': self.keyword_lin(x_dict['keyword']).relu(),
            'stock': self.stock_lin(x_dict['stock']).relu()
        }

        # Layer 1
        x1_dict = self.conv1(x_dict, edge_index_dict)
        for k in x1_dict.keys():
            x1_dict[k] = self.bn1(x1_dict[k]).relu()
            # [변경] Config의 dropout_lin 사용
            x1_dict[k] = F.dropout(x1_dict[k], p=self.dropout_lin_ratio, training=self.training)

        # Layer 2
        x2_dict = self.conv2(x1_dict, edge_index_dict)
        for k in x2_dict.keys():
            if k in x1_dict and x2_dict[k].shape == x1_dict[k].shape:
                x2_dict[k] = x2_dict[k] + x1_dict[k] 
            x2_dict[k] = self.bn2(x2_dict[k]).relu()
            # [변경] Config의 dropout_lin 사용
            x2_dict[k] = F.dropout(x2_dict[k], p=self.dropout_lin_ratio, training=self.training)

        # Layer 3
        x_out = self.conv3(x2_dict, edge_index_dict)
        
        return x_out

class CosineDecoder(torch.nn.Module):
    def __init__(self):
        super().__init__()
        
    def forward(self, z_src, z_dst):
        z_src = F.normalize(z_src, p=2, dim=1)
        z_dst = F.normalize(z_dst, p=2, dim=1)
        return (z_src * z_dst).sum(dim=-1)

class Model(torch.nn.Module):
    def __init__(self, hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim, config=None): # [변경] config 받음
        super().__init__()
        
        # Config에서 값 추출 (없으면 기본값)
        if config is None: config = {}
        model_conf = config.get('model', {})
        
        heads = model_conf.get('heads', 4)
        dropout = model_conf.get('dropout', 0.2)
        dropout_lin = model_conf.get('dropout_lin', 0.4)

        self.encoder = GNNEncoder(
            hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim,
            heads=heads, dropout=dropout, dropout_lin=dropout_lin
        )
        
        self.decoders = torch.nn.ModuleDict({
            'has_keyword': CosineDecoder(),
            'has_stock': CosineDecoder(),
            'stock_related_stock': CosineDecoder(),
            'keyword_related_stock': CosineDecoder()
        })

    def forward(self, x_dict, edge_index_dict, edge_label_index_dict=None):
        z_dict = self.encoder(x_dict, edge_index_dict)
        
        if edge_label_index_dict:
            preds = {}
            for edge_type, edge_index in edge_label_index_dict.items():
                src_type, _, dst_type = edge_type
                decoder_key = f"has_{dst_type}"
                
                if decoder_key in self.decoders:
                    row, col = edge_index
                    z_src = z_dict[src_type][row]
                    z_dst = z_dict[dst_type][col]
                    preds[edge_type] = self.decoders[decoder_key](z_src, z_dst)
            return preds
            
        return z_dict