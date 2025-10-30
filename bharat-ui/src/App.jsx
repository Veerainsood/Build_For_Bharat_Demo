import React, { useEffect, useRef, useState } from "react";
import "./bharat.css";
import "./chat.css";

export default function BharatUI() {
  const [loading, setLoading] = useState(false);
  const [question, setQuestion] = useState("");
  const [currentResponse, setCurrentResponse] = useState([]);
  const [currentStage, setCurrentStage] = useState("");
  const scrollRef = useRef(null);

  const stageOrder = [
    "status",
    "family",
    "datasets",
    "registry",
    "head1",
    "head2",
    "head3",
    "done",
  ];

  const ask = async (q) => {
    if (!q.trim() || loading) return;
    setLoading(true);
    setCurrentResponse([]);
    setCurrentStage("status");

    const evt = new EventSource(`http://127.0.0.1:8000/query?query=${encodeURIComponent(q)}`);

    const pushStage = (type, data) => {
      setCurrentResponse((r) => [...r, { type, data }]);
      setCurrentStage(type);
    };

    stageOrder.forEach((t) =>
      evt.addEventListener(t, (e) => pushStage(t, JSON.parse(e.data)))
    );

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

  // smooth scroll to bottom on new message
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [currentResponse]);

  const currentIndex = stageOrder.indexOf(currentStage);

  return (
    <div className="bharat-root">
      <div className="bg-animated"></div>

      <header className="header">
        <h1 className="title">Build for Bharat</h1>
        <p className="subtitle">Intelligence Pipeline Interface</p>
      </header>

      <div className="content-container">
        {/* floating output box */}
        {currentResponse.length > 0 && (
          <div className="floating-box">
            <div className="scrollable-content" ref={scrollRef}>
              {currentResponse.map((s, i) => (
                <div key={i} className="stage-line">
                  <strong>{s.type.toUpperCase()}</strong>
                  <pre>{JSON.stringify(s.data, null, 2)}</pre>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* right-side pipeline buttons */}
        <div className="right-stages-fixed">
          {stageOrder.map((s, i) => {
            const isDoneOrCurrent = i <= currentIndex;  // all past + current = blue
            const isNext = i === currentIndex + 1;      // next = yellow pulse
            const className = isDoneOrCurrent
              ? "stage-btn active"
              : isNext
              ? "stage-btn upcoming"
              : "stage-btn idle";

            return (
              <div key={s} className={className}>
                {s.toUpperCase()}
              </div>
            );
          })}
        </div>
      </div>

      {/* input area */}
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
          <button
            onClick={() => ask(question)}
            disabled={loading}
            className="button"
          >
            {loading ? "Running..." : "Ask"}
          </button>
        </div>
      </div>
    </div>
  );
}
