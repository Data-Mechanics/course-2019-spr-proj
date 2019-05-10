import React, { Component } from 'react';
import Visualizations from './Visualizations/Visualizations';
import Navbar from 'react-bootstrap/Navbar';


class App extends Component {  
  render() {    
    return (  
        <div>
          <Navbar bg="light">
            <Navbar.Brand>CS504 Dashboard</Navbar.Brand>
          </Navbar>
          <Visualizations/>
        </div> 
      );  
    }
}

export default App;