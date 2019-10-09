from typing import Iterable, Mapping

from modelforge import Model, merge_strings, register_model, split_strings

from sourced.ml.core.models.license import DEFAULT_LICENSE

import xgboost


@register_model
class BotDetection(Model):
    NAME = "bot-detection"
    VENDOR = "source{d}"
    DESCRIPTION = "Model that is used to identify bots among developer identities."
    LICENSE = DEFAULT_LICENSE

    def construct(self, booster: "xgboost.core.Booster", params: Mapping[str, int],
                  bpe_tokens: Iterable[str]):
        self._booster = booster
        self._params = params
        self._bpe_tokens = bpe_tokens
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
        dict with the parameters used to train the model.
        """
        return self._params

    @property
    def bpe_tokens(self):
        """
        List with the tokens composing the BPE model.
        The i-th token in the list corresponds to i-th token of the model.
        """
        return self._bpe_tokens

    def _generate_tree(self):
        return {"booster": bytes(self.booster.save_raw()),
                "params": self.params,
                "bpe_tokens": merge_strings(self.bpe_tokens)}

    def _load_tree(self, tree: dict):
        booster = xgboost.Booster()
        booster.load_model(bytearray(tree["booster"]))
        self.construct(booster=booster,
                       params=tree["params"],
                       bpe_tokens=split_strings(tree["bpe_tokens"]))
