# LevelDB 仿写
用C++20的module对LevelDB进行仿写，尽量用现代C++的语法实现，性能不会是第一考量

## 物理设计
1. 单体测试只有一个入口，即unit_test，剩余的.test.cpp文件都只有测试case而不会提供入口
2. module文件以.cppm结尾，.test.cpp为单元测试
  > 这里也是我选择doctest的原因。别的测试库很容易产生多个测试入口，不方便管理
3. 如果存在实现文件，以.impl.cpp结尾
4. 文件名必须是{project_name}_{component_name}.cppm 的形式，其中project固定为leveldb
5. 每一个cppm都是一个组件单元

## 依赖
1. xin::base: 我自己写的组件库，提供了一些很通用的小组件(通过git submodule引入)
2. doctest: 作为单体测试组件(通过vcpkg引入)

## 进度
1. [x] skip list
2. [x] write batch
3. [ ] memory table iterator未完成