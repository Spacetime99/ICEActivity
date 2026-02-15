import React from "react";
import ReactDOM from "react-dom/client";
import StatsPage from "./StatsPage";
import "./index.css";
import "./App.css";
import "./headlines.css";
import "./stats.css";

ReactDOM.createRoot(document.getElementById("stats-root")!).render(
  <React.StrictMode>
    <StatsPage />
  </React.StrictMode>,
);
