<template>
<div>
    <div id="cmdModal" class="modal">
        <div class="modal-content">
            <div class="close-cross"><span class="closeCmd">&times;</span></div>
            <div class="head">
                <h5 class="modal-title"><i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i>&nbsp; Workbase Commands &nbsp;<i style="font-size:35px;" class="pe page-header-icon pe-7s-rocket"></i></h5>
            </div>
            <table class="table table-hover table-stripped">
                <tbody>
                    <tr>
                        <td style="text-align:center;">
                            <i class="far fa-star"></i>
                            Starred Queries
                        </td>  
                        <td>
                            List all the saved queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Types 
                            <caret-icon style="vertical-align:middle;"></caret-icon>
                        </td>  
                        <td>
                            Drop down list to show what is in the schema
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <img src="static/img/icons/icon_add_white.svg">
                        </td>  
                        <td>
                            Save current query to starred queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="fas fa-chevron-circle-right"></i>
                            OR "ENTER"
                        </td>  
                        <td>
                            To visualize knowlege graph
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="far fa-times-circle"></i>
                        </td>  
                        <td>
                            To clear the query
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
                            limit Query
                        </td>  
                        <td>
                            Can be used to limit the number of results returned
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            limit neighbours
                        </td>  
                        <td>
                            Can be used to limit the number of results linked to a node
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Autoload role players
                        </td>  
                        <td>
                            Loads the role players of a relationship
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Single click on node
                        </td>  
                        <td>
                            Bring up information about node
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Double click on node
                        </td>  
                        <td>
                            Show relationships associated with the node
                        </td>                             
                    </tr> 
                    <tr>
                        <td style="text-align:center;">
                            Right click
                        </td>  
                        <td>
                            Open graph menu
                        </td>                             
                    </tr>  
                    <tr>
                        <td style="text-align:center;">
                            {{crtlOrCmd}} + click on nodes
                        </td>  
                        <td>
                            Select multiple nodes
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
    width: 700px;
}

td {
    border-bottom: 1px solid #606060;
    padding-bottom: 5px;
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
    width: 70%;
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
import CaretIcon from '@/components/UIElements/CaretIcon.vue';

export default {
  name: 'CommandsModal',
  components: { CaretIcon },
  data() {
    return {
      crtlOrCmd: 'ctrl',
    };
  },
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
        if (process.platform === 'darwin') {
          this.crtlOrCmd = 'Cmd';
        }
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
