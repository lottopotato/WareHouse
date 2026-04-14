def count_parameters(model):
    n_parameters = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f'{n_parameters:,}')