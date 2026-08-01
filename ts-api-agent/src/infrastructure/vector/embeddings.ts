export async function generateOllamaEmbedding(
  text: string,
  model: string = 'nomic-embed-text'
): Promise<number[]> {
  const ollamaUrl = process.env.OLLAMA_HOST || 'http://localhost:11434';
  
  try {
    const res = await fetch(`${ollamaUrl}/api/embeddings`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model,
        prompt: text,
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Ollama API error (${res.status}): ${errText}`);
    }

    const data: any = await res.json();
    if (!data.embedding || !Array.isArray(data.embedding)) {
      throw new Error('Ollama returned empty or invalid embedding vector');
    }

    return data.embedding;
  } catch (err: any) {
    console.error(`[Ollama Embedding Error]: ${err.message}`);
    throw err;
  }
}
