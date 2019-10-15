from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping

from modelforge import Model, register_model

from sourced.ml.core.models.license import DEFAULT_LICENSE

import xgboost

import youtokentome


@register_model
class BotDetection(Model):
    NAME = "bot-detection"
    VENDOR = "source{d}"
    DESCRIPTION = "Model that is used to identify bots among developer identities."
    LICENSE = DEFAULT_LICENSE

    def construct(self, booster: "xgboost.core.Booster", params: Mapping[str, int],
                  bpe_model_path: str):
        self._booster = booster
        self._params = params
        self._bpe_model_path = bpe_model_path
        self._bpe_model = youtokentome.BPE(bpe_model_path)
        return self

    @property
    def booster(self):
        """
        Returns the Booster model of XGBoost.
        """
        return self._booster

    @property
    def params(self):
        """
        Returns a dict with the parameters used to train the model.
        """
        return self._params

    @property
    def bpe_model(self):
        """
        Returns the BPE model.
        """
        return self._bpe_model

    def _generate_tree(self):
        return {"booster": bytes(self.booster.save_raw()),
                "params": self.params,
                "bpe_model": Path(self._bpe_model_path).read_text(encoding="utf8")}

    def _load_tree(self, tree: dict):
        booster = xgboost.Booster()
        booster.load_model(bytearray(tree["booster"]))
        with NamedTemporaryFile(mode="w") as f:
            f.write(tree["bpe_model"])
            f.seek(0)
            self.construct(booster=booster, params=tree["params"], bpe_model_path=f.name)
