import React from "react";
import ReactDOM from "react-dom/client";
import HeadlinesPage from "./HeadlinesPage";
import "./index.css";
import "./App.css";
import "./headlines.css";

ReactDOM.createRoot(document.getElementById("headlines-root")!).render(
  <React.StrictMode>
    <HeadlinesPage />
  </React.StrictMode>,
);
