<template>
    <div class="panel-container">
        <div @click="toggleContent" class="panel-header">
            <vue-icon :icon="(showRelationshipsPanel) ?  'chevron-down' : 'chevron-right'" iconSize="14" className="vue-icon"></vue-icon>
            <h1>Relationships</h1>
        </div>

        <div v-show="showRelationshipsPanel">

            <div class="content noselect" v-if="!currentKeyspace">
                Please select a keyspace
            </div>
            <div class="content noselect" v-else-if="(!selectedNodes || selectedNodes.length > 1)">
                Please select a node
            </div>

            <div class="content" v-else>

                <div class="row plays-row">
                    <div class="label">
                        Plays
                    </div>
                    <div class="value">
                        <div v-bind:class="(showRolesList) ? 'btn role-btn role-list-shown' : 'btn role-btn'" @click="toggleRoleList"><div class="role-btn-text" >{{currentRole}}</div><vue-icon class="role-btn-caret" className="vue-icon" icon="caret-down"></vue-icon></div>
                    </div>
                </div>

                <div class="panel-list-item">
                    <div class="role-list" v-show="showRolesList">
                        <ul v-for="role in Object.keys(relationships)" :key="role">
                            <li class="role-item" @click="selectRole(role)" v-bind:class="[(role === currentRole) ? 'role-item-selected' : '']">{{role}}</li>
                        </ul>
                    </div>
                </div>

                <div v-if="showRolePLayers" v-for="rel in relationships[currentRole]" :key="rel">
                    <div class="column">
                        <div class="row content-item">
                            <div class="label">
                                In
                            </div>
                            <div class="value">
                                {{rel}}
                            </div>
                            <!-- <div class="btn right-bar-btn" @click="loadRolePlayers(rel)"><vue-icon icon="more" className="vue-icon" iconSize="12"></vue-icon></div> -->
                        </div>

                        <div class="row content-item">
                            <div class="label">
                                Where
                            </div>
                            <div class="value">
                            </div>
                        </div>
                        

                        <div v-if="showRolePLayers" class="roleplayers-list content-item" v-for="(rp, index) in roleplayers[rel]" :key="index">
                            <div class="label">
                                {{rp.role}}
                            </div>
                            <div class="value">
                                {{rp.player}}
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- <div class="content noselect" v-if="!(relationship.size)">
                Please select a node
            </div> -->

        </div>

    </div>
</template>

<script>
  import { createNamespacedHelpers } from 'vuex';
  import { OPEN_GRAKN_TX } from '@/components/shared/StoresActions';


  export default {
    name: 'RelationshipsPanel',
    props: ['tabId'],
    data() {
      return {
        showRelationshipsPanel: true,
        showRolesList: false,
        currentRole: '',
        relationships: {},
        showRolePLayers: false,
        roleplayers: {},
      };
    },
    beforeCreate() {
      const { mapGetters, mapActions } = createNamespacedHelpers(`tab-${this.$options.propsData.tabId}`);

      // computed
      this.$options.computed = {
        ...(this.$options.computed || {}),
        ...mapGetters(['selectedNodes', 'currentKeyspace']),
      };

      // methods
      this.$options.methods = {
        ...(this.$options.methods || {}),
        ...mapActions([OPEN_GRAKN_TX]),
      };
    },
    watch: {
      async selectedNodes() {
        this.roleplayers = {};
        await this.loadRolesAndRelationships();
      },
      async currentRole() {
        await this.loadRolePlayers(this.relationships[this.currentRole][0]);
      },
    },
    methods: {
      toggleContent() {
        this.showRelationshipsPanel = !this.showRelationshipsPanel;
      },
      toggleRoleList() {
        this.showRolesList = !this.showRolesList;
      },
      selectRole(role) {
        this.showRolesList = false;
        this.currentRole = role;
      },
      async loadRolesAndRelationships() {
        // Initialize relationships map
        this.relationships = {};

        const graknTx = await this[OPEN_GRAKN_TX]();

        const node = await graknTx.getConcept(this.selectedNodes[0].id);

        const roles = await (await node.roles()).collect();

        // Map roles to their respective relationships
        await Promise.all(roles.map(async (x) => {
          const roleLabel = await x.label();
          if (!(roleLabel in this.relationships)) {
            this.relationships[roleLabel] = await Promise.all((await (await x.relationships()).collect()).map(async rel => rel.label()));
          }
        }));
        this.currentRole = Object.keys(this.relationships)[0];

        graknTx.close();
      },
      async loadRolePlayers(rel) {
        this.showRolePLayers = false;

        // If roleplayers have not already been retrieved
        if (!this.roleplayers[rel]) {
          const graknTx = await this[OPEN_GRAKN_TX]();

          const node = await graknTx.getConcept(this.selectedNodes[0].id);

          const roles = await (await node.roles()).collect();

          // Get role concept of selected current role
          const role = await (Promise.all(roles.map(async x => ((await x.label() === this.currentRole) ? x : null)))).then(roles => roles.filter(r => r));

          // Get relationship concepts of current role
          let relationships = await (await node.relationships(...role[0])).collect();

          // Filter relationships
          relationships = await (Promise.all(relationships.map(async x => ((await (await x.type()).label() === rel) ? x : null)))).then(rels => rels.filter(r => r));

          // For every relationship, map relationships to their respective roleplayer and the role it plays
          await Promise.all(relationships.map(async (x) => {
            let roleplayers = await x.rolePlayersMap();
            roleplayers = Array.from(roleplayers.entries());

            await Promise.all(Array.from(roleplayers, async ([role, setOfThings]) => {
              const roleLabel = await role.label();
              await Promise.all(Array.from(setOfThings.values())
                .map(async (thing) => {
                  const thingLabel = await (await thing.type()).label();
                  if (roleLabel !== this.currentRole) {
                    if (!this.roleplayers[rel]) this.roleplayers[rel] = []; // Initialize array only for the first roleplayer
                    this.roleplayers[rel].push({ role: roleLabel, player: `${thingLabel}: ${thing.id}` });
                  }
                }));
            }));
          }));
          graknTx.close();
        }
        this.showRolePLayers = true;
      },
    },
  };
</script>

<style scoped>

    .plays-row {
        padding-left: var(--container-padding);
        padding-right: var(--container-padding);
    }

    .column {
        display: flex;
        flex-direction: column;
        align-items: center;
    }

    .row {
        display: flex;
        flex-direction: row;
        align-items: center;
    }

    .roleplayers-list {
        display: flex;
        flex-direction: row;
        align-items: center;
        padding: 3px;
    }

    .content {
        padding: var(--container-padding);
        display: flex;
        flex-direction: column;
        max-height: 300px;
        overflow: scroll;
        justify-content: center;
        border-bottom: var(--container-darkest-border);
    }

    .content-item {
        padding: var(--container-padding);
        display: flex;
        flex-direction: row;
        align-items: center;
    }

    .label {
        margin-right: 20px;
        width: 65px;
    }

    .value {
        width: 110px;
    }

    .role-btn {
        height: 22px;
        min-height: 22px !important;
        cursor: pointer;
        display: flex;
        flex-direction: row;
        width: 100%;
        margin: 0px !important;
        z-index: 2;
    }

    .role-btn-text {
        width: 100%;
        padding-left: 4px;
        display: block;
        white-space: normal !important;
        word-wrap: break-word;
        line-height: 19px;
    }

    .role-btn-caret {
        cursor: pointer;
        align-items: center;
        display: flex;
        min-height: 22px;
        margin: 0px !important;
    }

    .role-list {
        border-left: var(--container-darkest-border);
        border-right: var(--container-darkest-border);
        border-bottom: var(--container-darkest-border);


        background-color: var(--gray-1);
        max-height: 137px;
        overflow: auto;
        position: absolute;
        width: 108px;
        right: 11px;
        margin-top: 0px;
        z-index: 1;
    }

    .role-list-shown {
        border: 1px solid var(--button-hover-border-color) !important;
    }


    .role-list::-webkit-scrollbar {
        width: 2px;
    }

    .role-list::-webkit-scrollbar-thumb {
        background: var(--green-4);
    }

    .role-item {
        align-items: center;
        padding: 2px;
        cursor: pointer;
        white-space: normal;
        word-wrap: break-word;
    }

    .role-item:hover {
        background-color: var(--purple-4);
    }

    /*dynamic*/
    .role-item-selected {
        background-color: var(--purple-3);
    }

</style>
