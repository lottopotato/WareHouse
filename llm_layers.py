import torch
import math

class LinearBlock(torch.nn.Module):
    def __init__(self, input_dims, output_dims, dropout = 0.1, activation = 'none'):
        super().__init__()
        self.linear = torch.nn.Linear(input_dims, output_dims)
        if activation == 'relu':
            self.activation = torch.nn.ReLU()
        elif activation == 'none':
            self.activation = None
        elif activation == 'sigmoid':
            self.activation = torch.nn.Sigmoid()
        elif activation == 'logSoftmax':
            self.activation = torch.nn.LogSoftmax(-1)
        else:
            self.activation = torch.nn.Tanh()
        self.dropout = torch.nn.Dropout(dropout) if dropout > 0 else None
    
    def forward(self, x):
        x = self.linear(x)
        if self.activation:
            x = self.activation(x)
        if self.dropout:
            x = self.dropout(x)
        return x

class PositionEncoding(torch.nn.Module):
    def __init__(self, hidden_size, maxlen,
                 dropout = 0.1, return_pos_embedding = False, device = None):
        super().__init__()
        den = torch.exp(- torch.arange(0, hidden_size, 2) * math.log(10000)/ hidden_size)
        pos = torch.arange(0, maxlen).reshape(maxlen, 1)
        
        pos_embedding = torch.zeros((maxlen, hidden_size))
        
        pos_embedding[:, 0::2] = torch.sin(pos * den)
        pos_embedding[:, 1::2] = torch.cos(pos * den)
        
        pos_embedding = pos_embedding.unsqueeze(0)
        if device:
            pos_embedding = pos_embedding.to(device)

        self.return_pos_embedding = return_pos_embedding
        
        self.dropout = torch.nn.Dropout(dropout)
        self.register_buffer('pos_embedding', pos_embedding)
                
        self.device = device