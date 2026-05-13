
class SimpleCNN3(nn.Module):
    def __init__(self):
        super(SimpleCNN3, self).__init__()
        self.encoding = nn.Sequential(
            nn.Conv3d(1, 1, 3),
            nn.MaxPool3d(2),
            nn.Sigmoid(),
            nn.Conv3d(1, 1, 3),
            nn.MaxPool3d(2),
            nn.Sigmoid()
        )
        self.decoding = nn.Sequential(
            nn.ConvTranspose3d(1, 1, 3,stride=2),
            nn.Sigmoid(),
            nn.ConvTranspose3d(1, 1, 3,stride=2),
            nn.Sigmoid(),
            nn.ConvTranspose3d(1, 1, 3),
            nn.Sigmoid(),
            nn.ConvTranspose3d(1, 1, 2),#3
            nn.Sigmoid()
        )

    def forward(self, x):
        x = self.encoding(x)
        x = self.decoding(x)
        return x
