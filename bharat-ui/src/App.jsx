import React, { useState } from "react";
import "./bharat.css";
import "./chat.css"
export default function BharatUI() {
  const [loading, setLoading] = useState(false);
  const [question, setQuestion] = useState("");
  const [currentResponse, setCurrentResponse] = useState([]);

  const ask = async (q) => {
    if (!q.trim() || loading) return;
    setLoading(true);
    setCurrentResponse([]); // clear previous content

    const evt = new EventSource(`http://127.0.0.1:8000/query?query=${encodeURIComponent(q)}`);

    const pushStage = (type, data) => {
      setCurrentResponse((r) => [...r, { type, data }]);
    };

    evt.addEventListener("status", (e) => pushStage("status", JSON.parse(e.data)));
    evt.addEventListener("family", (e) => pushStage("family", JSON.parse(e.data)));
    evt.addEventListener("datasets", (e) => pushStage("datasets", JSON.parse(e.data)));
    evt.addEventListener("registry", (e) => pushStage("registry", JSON.parse(e.data)));
    evt.addEventListener("head1", (e) => pushStage("head1", JSON.parse(e.data)));
    evt.addEventListener("head2", (e) => pushStage("head2", JSON.parse(e.data)));
    evt.addEventListener("head3", (e) => pushStage("head3", JSON.parse(e.data)));
    evt.addEventListener("done", (e) => {
      pushStage("done", JSON.parse(e.data));
      evt.close();
      setLoading(false);
    });
    evt.onerror = () => {
      evt.close();
      setLoading(false);
    };
  };

  return (
    <div className="bharat-root">
      <div className="bg-animated"></div>

      <header className="header">
        <h1 className="title">Build for Bharat</h1>
        <p className="subtitle">Intelligence Pipeline Interface</p>
      </header>

      {/* centered floating result box */}
      {currentResponse.length > 0 && (
        <div className="floating-box">
          <div className="scrollable-content">
            {currentResponse.map((s, i) => (
              <div key={i} className="stage-line">
                <strong>{s.type.toUpperCase()}</strong>
                <pre>{JSON.stringify(s.data, null, 2)}</pre>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* fixed bottom query box */}
      <div className="query-box fixed">
        <h2 className="query-title">Ask anything</h2>
        <div className="input-row">
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && ask(question)}
            placeholder="e.g., Compare rainfall with crop yield..."
            className="input"
            disabled={loading}
          />
          <button onClick={() => ask(question)} disabled={loading} className="button">
            {loading ? "Running..." : "Ask"}
          </button>
        </div>
      </div>
    </div>
  );
}
