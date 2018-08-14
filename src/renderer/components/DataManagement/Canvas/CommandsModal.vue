<template>
<div>
    <div id="cmdModal" class="modal">
        <div class="modal-content">
            <div class="close-cross"><span class="closeCmd">&times;</span></div>
            <div class="head">
                <h5 class="modal-title">&nbsp; workbase commands &nbsp;</h5>
            </div>
            <nav class="nav" role="navigation">
                <ul class="nav-list">
                    <li v-bind:id="tab" :class="{'active':activeTab===tab}" v-for="tab in tabs" @click="activeTab=tab" :key="tab">{{tab}}</li>
                </ul>
            </nav>
            <table class="table table-hover table-stripped" v-if="activeTab === 'graql editor'">
                <tbody>
                    <tr>
                        <td style="text-align:center;">
                            <i class="far fa-star"></i>
                        </td>  
                        <td>
                            list all the saved queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            Types 
                            <caret-icon style="vertical-align:middle;"></caret-icon>
                        </td>  
                        <td>
                            drop down list to show what is in the schema
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            history
                        </td>  
                        <td>
                            use [SHIFT] + [UP/DOWN] to navigate through previous graql queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <img src="static/img/icons/icon_add_white.svg">
                        </td>  
                        <td>
                            save current query to starred queries
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="fas fa-chevron-circle-right"></i>
                            or [ENTER]
                        </td>  
                        <td>
                            to visualize knowlege graph
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="far fa-times-circle"></i>
                        </td>  
                        <td>
                            to clear the query
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            <i class="fa fa-cog"></i>
                        </td>  
                        <td>
                            toggle query settings
                        </td>                             
                    </tr>             
                    <tr>
                        <td style="text-align:center;">
                            limit query
                        </td>  
                        <td>
                            can be used to limit the number of results returned
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            limit neighbours
                        </td>  
                        <td>
                            can be used to limit the number of results linked to a node
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            autoload role players
                        </td>  
                        <td>
                            load the role players of a relationship
                        </td>                             
                    </tr>
                </tbody>
            </table>
            <table class="table table-hover table-stripped" v-if="activeTab === 'graph'">
                <tbody>
                    <tr>
                        <td style="text-align:center;">
                            single click on a node
                        </td>  
                        <td>
                            select a node and open its node panel showing information about it
                        </td>                             
                    </tr>
                    <tr>
                        <td style="text-align:center;">
                            double click on a node
                        </td>  
                        <td>
                            load neighbours associated with the node
                        </td>                             
                    </tr> 
                    <tr>
                        <td style="text-align:center;">
                            [SHIFT] + double click
                        </td>  
                        <td>
                            load attributes associated with the node
                        </td>                             
                    </tr> 
                    <tr>
                        <td style="text-align:center;">
                            right click on a node
                        </td>  
                        <td>
                            open graph menu
                        </td>                             
                    </tr>  
                    <tr>
                        <td style="text-align:center;">
                            [{{crtlOrCmd}}] + click on a node
                        </td>  
                        <td>
                            select multiple nodes
                        </td>                             
                    </tr>  
                    <tr>
                        <td style="text-align:center;">
                            click + hold on a node
                        </td>  
                        <td>
                            select multiple nodes
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
    width: 760px;
}

td {
    border-bottom: 1px solid #606060;
    padding-bottom: 5px;
}

.table-responsive {
    margin-top: 10px;
}

.graqlEditor-wrapper {
    z-index: 3;
    display: flex;
    flex-direction: column;
    flex: 1;
    border-radius: 3px;
    position: absolute;
    width: 100%;
    top: -15px;
}

.nav-list{
  display: flex;
  flex-direction: row;
  justify-content: center;
}
.active{
  border-bottom: 2px solid #00eca2;
}

li{
  cursor: pointer;
  margin: 40px;
}

li:hover,
li:focus {
    color: #00eca2;
    text-decoration: none;
    cursor: pointer;
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
    animation-duration: 0.4s;
    height: 650px;
    margin-top: 5%;
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
    const tabsArray = ['graql editor', 'graph'];
    return {
      tabs: tabsArray,
      activeTab: tabsArray[0],
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
          this.crtlOrCmd = 'cmd';
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
