import torch
from torch_geometric.nn import SAGEConv, HeteroConv


class GNNEncoder(torch.nn.Module):
    # 🟢 [수정] 인자 변경: num_keywords/stocks (개수) -> keyword/stock_feat_dim (벡터 차원 크기)
    def __init__(self, hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim):
        super().__init__()

        # 🟢 [수정] Embedding -> Linear 변경
        # 기존: ID를 받아 벡터로 변환 (학습)
        # 변경: 이미 있는 벡터를 받아 차원만 변환 (Projection)
        self.news_lin = torch.nn.Linear(news_feat_dim, hidden_dim)
        self.keyword_lin = torch.nn.Linear(keyword_feat_dim, hidden_dim)
        self.stock_lin = torch.nn.Linear(stock_feat_dim, hidden_dim)

        # Conv Layers
        self.conv1 = HeteroConv({
            ('news', 'has_keyword', 'keyword'): SAGEConv(-1, hidden_dim),
            ('keyword', 'rev_has_keyword', 'news'): SAGEConv(-1, hidden_dim),
            ('news', 'has_stock', 'stock'): SAGEConv(-1, hidden_dim),
            ('stock', 'rev_has_stock', 'news'): SAGEConv(-1, hidden_dim),
        }, aggr='sum')

        self.conv2 = HeteroConv({
            ('news', 'has_keyword', 'keyword'): SAGEConv(-1, out_dim),
            ('keyword', 'rev_has_keyword', 'news'): SAGEConv(-1, out_dim),
            ('news', 'has_stock', 'stock'): SAGEConv(-1, out_dim),
            ('stock', 'rev_has_stock', 'news'): SAGEConv(-1, out_dim),
        }, aggr='sum')

    def forward(self, x_dict, edge_index_dict):
        # 🟢 [수정] Linear Layer 적용
        x_dict_out = {
            'news': self.news_lin(x_dict['news']).relu(),
            'keyword': self.keyword_lin(x_dict['keyword']).relu(),
            'stock': self.stock_lin(x_dict['stock']).relu()
        }

        # Message Passing
        x_dict_out = self.conv1(x_dict_out, edge_index_dict)
        x_dict_out = {key: x.relu() for key, x in x_dict_out.items()}

        x_dict_out = self.conv2(x_dict_out, edge_index_dict)
        return x_dict_out


class EdgeDecoder(torch.nn.Module):
    def __init__(self, hidden_dim):
        super().__init__()
        self.lin1 = torch.nn.Linear(2 * hidden_dim, hidden_dim)
        self.lin2 = torch.nn.Linear(hidden_dim, 1)

    def forward(self, z_src, z_dst):
        z = torch.cat([z_src, z_dst], dim=-1)
        z = self.lin1(z).relu()
        z = self.lin2(z)
        return z.view(-1)


class Model(torch.nn.Module):
    # 🟢 [수정] 인자 이름 변경
    def __init__(self, hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim):
        super().__init__()
        self.encoder = GNNEncoder(hidden_dim, out_dim, news_feat_dim, keyword_feat_dim, stock_feat_dim)
        self.decoders = torch.nn.ModuleDict({
            'has_keyword': EdgeDecoder(out_dim),
            'has_stock': EdgeDecoder(out_dim),
        })

    def forward(self, x_dict, edge_index_dict, edge_label_index_dict):
        z_dict = self.encoder(x_dict, edge_index_dict)
        preds = {}
        for edge_type, edge_label_index in edge_label_index_dict.items():
            src_type, rel_type, dst_type = edge_type
            if rel_type in self.decoders:
                z_src = z_dict[src_type][edge_label_index[0]]
                z_dst = z_dict[dst_type][edge_label_index[1]]
                preds[edge_type] = self.decoders[rel_type](z_src, z_dst)
        return preds