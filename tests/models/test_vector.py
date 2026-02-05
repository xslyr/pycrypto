import pytest

from pycrypto.models.vector import Documento


def test_documento_instantiation():
    """Valida se o modelo Documento pode ser instanciado como dataclass."""
    data = {
        "id": 1,
        "content": "Conteúdo de teste para RAG",
        "metadata_": {"source": "binance_docs", "priority": 1},
        "embedding": [0.1] * 768,  # Simula vetor de 768 dimensões
    }

    doc = Documento(**data)

    assert doc.id == 1
    assert doc.content == "Conteúdo de teste para RAG"
    assert doc.metadata_["source"] == "binance_docs"
    assert len(doc.embedding) == 768
