<!--
Grakn - A Distributed Semantic Database
Copyright (C) 2016  Grakn Labs Limited

Grakn is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Grakn is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>. -->


<template>
<span v-if="editorLinesNumber>1" @click="toggleEditorCollapse"><i :class="[isEditorCollapsed ? 'pe-7s-angle-down-circle' : 'pe-7s-angle-up-circle']"></i></span>
</template>

<style scoped>
span {
    color: #56C0E0;
    font-size: 25px;
    cursor: pointer;
    margin: auto;
    display: inline-flex;
}
</style>

<script>
export default {
    name: "scrollButton",
    props: ['editorLinesNumber', 'codeMirror'],
    data() {
        return {
            initialEditorHeight: undefined,
            isEditorCollapsed: false,
        }
    },

    created() {},
    watch: {
        editorLinesNumber: function(newVal, oldVal) {
            //Set auto height when going back to 1 line and reset the boolean
            if (newVal === 1) {
                $(".CodeMirror").css({
                    'height': 'auto'
                });
                this.isEditorCollapsed = false;
            }

        }
    },
    mounted: function() {
        this.$nextTick(function() {
            $(document).ready(() => {
                this.initialEditorHeight = $(".CodeMirror").height();
                this.codeMirror.on("focus", (codeMirrorObj, changeObj) => {
                    if (this.isEditorCollapsed) {
                        $(".CodeMirror").animate({
                            height: $(".CodeMirror-sizer").outerHeight()
                        }, 300, function() {
                            $(".CodeMirror").css({
                                'height': 'auto'
                            });
                        });
                        this.isEditorCollapsed = false;
                    }
                });
            });


        });
    },

    methods: {
        toggleEditorCollapse() {
            if (!this.isEditorCollapsed) {
                $(".CodeMirror").animate({
                    height: this.initialEditorHeight
                }, 300);
                this.isEditorCollapsed = true;
            } else {
                $(".CodeMirror").animate({
                    height: $(".CodeMirror-sizer").outerHeight()
                }, 300, function() {
                    $(".CodeMirror").css({
                        'height': 'auto'
                    });
                });
                this.isEditorCollapsed = false;
            }
        }
    }
}
</script>
