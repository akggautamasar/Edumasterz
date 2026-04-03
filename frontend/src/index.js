import React from 'react';
import ReactDOM from 'react-dom/client';

function App() {
  // Redirect to backend homepage since this is just a placeholder
  React.useEffect(() => {
    window.location.href = '/';
  }, []);
  
  return (
    <div style={{ 
      display: 'flex', 
      justifyContent: 'center', 
      alignItems: 'center', 
      height: '100vh',
      fontFamily: 'system-ui, sans-serif'
    }}>
      <p>Redirecting to TG Drive...</p>
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
