<!--
GRAKN.AI - THE KNOWLEDGE GRAPH
Copyright (C) 2018 Grakn Labs Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
-->

<template>
<div>
    <div id="cmdModal" class="modal">
        <div class="modal-content">
            <div class="close-cross"><span class="closeCmd">&times;</span></div>
            <div class="head">
                <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Grakn commands &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
            </div>
            <table class="table table-hover table-stripped">
                <tbody>
                    <tr>
                        <td style="text-align:center;">
                            <i class="fa fa-star-o"></i>
                            Starred Queries
                        </td>  
                        <td>
                            Will list all the saved queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Types 
                            <i class="pe-7s-angle-down-circle"></i>
                        </td>  
                        <td>
                            Drop down list to show what is in the schema
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="pe-7s-angle-right-circle"></i>
                            OR "ENTER"
                        </td>  
                        <td>
                            To visualize knowlege graph
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="pe-7s-close-circle"></i>
                        </td>  
                        <td>
                            To clear the query
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            "SHIFT" + 
                            <i class="pe-7s-close-circle"></i>
                        </td>  
                        <td>
                            To clear the entire knowlege graph
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="fa fa-cog"></i>
                        </td>  
                        <td>
                            Toggle query settings
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Lock nodes position
                        </td>  
                        <td>
                            Organize nodes and reveal their connections without them jumping out of place
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Activate inference
                        </td>  
                        <td>
                            It is on by default per query, but turn this off when you need to run queries that use inference
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Query limit
                        </td>  
                        <td>
                            Can be used to limit the number of results returned
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Keyspace
                        </td>  
                        <td>
                            Keyspace selector
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Single left click on node
                        </td>  
                        <td>
                            Bring up information about node
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Double left click on node
                        </td>  
                        <td>
                            Show relationships associated with the node
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Single left click + hold on node
                        </td>  
                        <td>
                            Modify node visualisation settings
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Hold shift + double left click on entitiy node
                        </td>  
                        <td>
                            Show all attributes associated with entity
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Hold shift + right click on node
                        </td>  
                        <td>
                            Remove node from graph
                        </td>                             
                    </tr>                    
                    <tr>
                        <td style="text-align:center;">
                            Hold Ctrl + left click on nodes
                        </td>  
                        <td>
                            Select multiple nodes
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Right click on node(s)
                        </td>  
                        <td>
                            To build queries or align nodes
                        </td>                             
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</div>
</template>

<style scoped>
.head {
    margin-bottom: 20px;
    font-weight: bold;
}

.close-cross {
    display: flex;
    justify-content: flex-end;
    width: 100%;
}

.close-cross span {
    color: white;
}

table {
    border-collapse: separate;
    border-spacing: 15px;
}

td {
    border-bottom: 1px solid #606060;
    padding: 5px;
}

.table-responsive {
    margin-top: 10px;
}

/* The Modal (background) */

.modal {
    display: none;
    /* Hidden by default */
    position: fixed;
    /* Stay in place */
    z-index: 10;
    /* Sit on top */
    padding-top: 80px;
    /* Location of the box */
    left: 0;
    top: 0;
    width: 100%;
    /* Full width */
    height: 100%;
    /* Full height */
    overflow: auto;
    /* Enable scroll if needed */
    background-color: rgb(0, 0, 0);
    /* Fallback color */
    background-color: rgba(0, 0, 0, 0.3);
    /* Black w/ opacity */
}


/* Modal Content */

.modal-content {
    position: relative;
    background-color: #0f0f0f;
    margin: auto;
    padding: 15px;
    width: 60%;
    box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2), 0 6px 20px 0 rgba(0, 0, 0, 0.19);
    display: flex;
    flex-direction: column;
    align-items: center;
    -webkit-animation-name: animatetop;
    -webkit-animation-duration: 0.4s;
    animation-name: animatetop;
    animation-duration: 0.4s
}


/* Add Animation */

@-webkit-keyframes animatetop {
    from {
        top: 300px;
        opacity: 0
    }
    to {
        top: 0;
        opacity: 1
    }
}

@keyframes animatetop {
    from {
        top: 300px;
        opacity: 0
    }
    to {
        top: 0;
        opacity: 1
    }
}


/* The Close Button */

.closeCmd {
    color: white;
    float: right;
    font-size: 28px;
    font-weight: bold;
}

.closeCmd:hover,
.closeCmd:focus {
    color: #00eca2;
    text-decoration: none;
    cursor: pointer;
}
</style>

<script>

export default {
  name: 'CommandsModal',    

  mounted() {
    this.$nextTick(() => {
            // Get the modal
      const cmdModal = document.getElementById('cmdModal');

            // Get the button that opens the modal
      const cmdBtn = document.getElementById('cmdBtn');

            // Get the <span> element that closes the modal
      const cmdSpan = document.getElementsByClassName('closeCmd')[0];

            // When the user clicks the button, open the modal
      cmdBtn.onclick = () => {
        cmdModal.style.display = 'block';
      };

            // When the user clicks on <span> (x), close the modal
      cmdSpan.onclick = () => {
        cmdModal.style.display = 'none';
      };

            // When the user clicks anywhere outside of the modal, close it
      window.onclick = (event) => {
        if (event.target === cmdModal) {
          cmdModal.style.display = 'none';
        }
      };
    });
  },
};
</script>
